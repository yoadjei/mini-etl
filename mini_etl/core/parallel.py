"""
Parallel and concurrent processing utilities for ETL pipelines.
"""

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Generator, Iterable, List, Optional
import logging
import queue
import threading

import pandas as pd

from mini_etl.core.base import Transformer

logger = logging.getLogger(__name__)


@dataclass
class ParallelConfig:
    """Configuration for parallel processing."""
    
    workers: int = 4
    use_processes: bool = False  # True = ProcessPoolExecutor, False = ThreadPoolExecutor
    ordered: bool = True  # Maintain chunk order in output
    queue_size: int = 100  # Max chunks to buffer
    
    def __post_init__(self):
        if self.workers < 1:
            raise ValueError("workers must be at least 1")
        if self.queue_size < 1:
            raise ValueError("queue_size must be at least 1")


class ParallelTransformer(Transformer):
    """
    Wraps a transformer to execute in parallel across chunks.
    
    This is useful for CPU-bound transformations that can benefit from
    multiple cores. I/O-bound operations should use ThreadPoolExecutor,
    while CPU-bound operations should use ProcessPoolExecutor.
    
    Example:
        transformer = FilterTransformer(lambda df: df['value'] > 100)
        parallel_t = ParallelTransformer(transformer, workers=4)
        
        # Use in pipeline
        pipeline.add_transformer(parallel_t)
    
    Note: When using ProcessPoolExecutor, the transformer and its
    dependencies must be picklable.
    """
    
    def __init__(
        self,
        transformer: Transformer,
        config: Optional[ParallelConfig] = None,
    ):
        self.transformer = transformer
        self.config = config or ParallelConfig()
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform a single chunk (delegates to wrapped transformer)."""
        return self.transformer.transform(data)
    
    def transform_stream(
        self, 
        stream: Iterable[pd.DataFrame],
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Transform a stream of chunks in parallel.
        
        Args:
            stream: Iterable of DataFrames.
        
        Yields:
            Transformed DataFrames, optionally in order.
        """
        executor_class = (
            ProcessPoolExecutor if self.config.use_processes 
            else ThreadPoolExecutor
        )
        
        with executor_class(max_workers=self.config.workers) as executor:
            if self.config.ordered:
                yield from self._transform_ordered(executor, stream)
            else:
                yield from self._transform_unordered(executor, stream)
    
    def _transform_ordered(
        self,
        executor,
        stream: Iterable[pd.DataFrame],
    ) -> Generator[pd.DataFrame, None, None]:
        """Transform chunks maintaining order."""
        futures = []
        
        for chunk in stream:
            future = executor.submit(self.transformer.transform, chunk)
            futures.append(future)
            
            # Yield completed futures from the front to maintain order
            while futures and futures[0].done():
                yield futures.pop(0).result()
        
        # Yield remaining
        for future in futures:
            yield future.result()
    
    def _transform_unordered(
        self,
        executor,
        stream: Iterable[pd.DataFrame],
    ) -> Generator[pd.DataFrame, None, None]:
        """Transform chunks without maintaining order (faster)."""
        futures = []
        
        for chunk in stream:
            futures.append(executor.submit(self.transformer.transform, chunk))
            
            # Limit pending futures
            if len(futures) >= self.config.queue_size:
                for completed in as_completed(futures[:self.config.workers]):
                    yield completed.result()
                    futures.remove(completed)
        
        # Yield remaining
        for future in as_completed(futures):
            yield future.result()


class StreamBuffer:
    """
    Thread-safe buffer for producer-consumer streaming pattern.
    
    Useful when you need to decouple reading and processing speeds.
    """
    
    def __init__(self, maxsize: int = 100):
        self._queue: queue.Queue = queue.Queue(maxsize=maxsize)
        self._finished = threading.Event()
    
    def put(self, item: Any, timeout: Optional[float] = None) -> None:
        """Add item to buffer."""
        self._queue.put(item, timeout=timeout)
    
    def get(self, timeout: Optional[float] = None) -> Any:
        """Get item from buffer."""
        return self._queue.get(timeout=timeout)
    
    def finish(self) -> None:
        """Signal that no more items will be added."""
        self._finished.set()
    
    def is_finished(self) -> bool:
        """Check if buffer is finished and empty."""
        return self._finished.is_set() and self._queue.empty()
    
    def __iter__(self):
        """Iterate over buffered items."""
        while True:
            try:
                yield self._queue.get(timeout=0.1)
            except queue.Empty:
                if self._finished.is_set():
                    break


def parallel_map(
    func: Callable[[pd.DataFrame], pd.DataFrame],
    chunks: Iterable[pd.DataFrame],
    workers: int = 4,
    use_processes: bool = False,
) -> Generator[pd.DataFrame, None, None]:
    """
    Apply a function to chunks in parallel.
    
    Simple functional interface for parallel processing.
    
    Args:
        func: Function to apply to each chunk.
        chunks: Iterable of DataFrames.
        workers: Number of parallel workers.
        use_processes: Use processes instead of threads.
    
    Yields:
        Transformed DataFrames.
    
    Example:
        def process(df):
            return df[df['value'] > 100]
        
        for result in parallel_map(process, chunks, workers=4):
            print(result.shape)
    """
    executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    
    with executor_class(max_workers=workers) as executor:
        futures = [executor.submit(func, chunk) for chunk in chunks]
        for future in futures:
            yield future.result()


class ChunkBalancer:
    """
    Balances chunk sizes for more even parallel processing.
    
    When chunks vary significantly in size, some workers may finish
    early while others are still processing. This balancer attempts
    to create more evenly-sized chunks.
    """
    
    def __init__(self, target_size: int = 10000):
        self.target_size = target_size
        self._buffer: Optional[pd.DataFrame] = None
    
    def balance(
        self, 
        stream: Iterable[pd.DataFrame],
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Re-chunk stream into more evenly sized chunks.
        
        Args:
            stream: Input chunk stream.
        
        Yields:
            Balanced chunks of approximately target_size rows.
        """
        for chunk in stream:
            if self._buffer is not None:
                chunk = pd.concat([self._buffer, chunk], ignore_index=True)
                self._buffer = None
            
            while len(chunk) >= self.target_size:
                yield chunk.iloc[:self.target_size]
                chunk = chunk.iloc[self.target_size:]
            
            if len(chunk) > 0:
                self._buffer = chunk
        
        # Yield remaining buffer
        if self._buffer is not None and len(self._buffer) > 0:
            yield self._buffer
            self._buffer = None
