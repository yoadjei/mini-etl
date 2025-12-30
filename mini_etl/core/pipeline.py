"""
Core pipeline orchestration with progress tracking.
"""

from typing import Callable, Generator, Iterable, List, Optional
import logging
import time

import pandas as pd
from tqdm import tqdm

from mini_etl.core.base import DataSource, Transformer, DataSink
from mini_etl.core.schema import Schema, SchemaValidator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Pipeline:
    """
    Main ETL pipeline orchestrator.
    
    Supports streaming execution with progress tracking, schema validation,
    and comprehensive error handling.
    
    Example:
        pipeline = Pipeline(name="sales_etl")
        
        pipeline.set_source(CSVExtractor("sales.csv"))\\
                .add_transformer(FilterTransformer(lambda df: df['amount'] > 0))\\
                .add_transformer(RenameTransformer({'amount': 'total'}))\\
                .set_sink(ParquetLoader("output.parquet"))
        
        stats = pipeline.run()
        print(f"Processed {stats['rows']} rows in {stats['duration']:.2f}s")
    """
    
    def __init__(
        self,
        name: str = "pipeline",
        show_progress: bool = True,
        validate_schema: bool = False,
        schema: Optional[Schema] = None,
        on_error: str = "raise",  # 'raise', 'skip', 'log'
    ):
        self.name = name
        self.show_progress = show_progress
        self.validate_schema = validate_schema
        self.schema = schema
        self.on_error = on_error
        
        self._source: Optional[DataSource] = None
        self._transformers: List[Transformer] = []
        self._sink: Optional[DataSink] = None
        self._row_count = 0
        self._chunk_count = 0
        self._error_count = 0
        
        # Callbacks
        self._on_chunk_start: Optional[Callable] = None
        self._on_chunk_complete: Optional[Callable] = None
        self._on_error: Optional[Callable] = None
    
    def set_source(self, source: DataSource) -> "Pipeline":
        """Set the data source."""
        if not isinstance(source, DataSource):
            raise TypeError("source must be a DataSource instance")
        self._source = source
        return self
    
    def add_transformer(self, transformer: Transformer) -> "Pipeline":
        """Add a transformer to the pipeline."""
        if not isinstance(transformer, Transformer):
            raise TypeError("transformer must be a Transformer instance")
        self._transformers.append(transformer)
        return self
    
    def set_sink(self, sink: DataSink) -> "Pipeline":
        """Set the data sink."""
        if not isinstance(sink, DataSink):
            raise TypeError("sink must be a DataSink instance")
        self._sink = sink
        return self
    
    def on_chunk_start(self, callback: Callable[[int], None]) -> "Pipeline":
        """Set callback called before each chunk is processed."""
        self._on_chunk_start = callback
        return self
    
    def on_chunk_complete(self, callback: Callable[[int, int], None]) -> "Pipeline":
        """Set callback called after each chunk is processed."""
        self._on_chunk_complete = callback
        return self
    
    def on_error_callback(self, callback: Callable[[Exception, int], None]) -> "Pipeline":
        """Set callback called when an error occurs."""
        self._on_error = callback
        return self
    
    def run(self) -> dict:
        """
        Execute the pipeline.
        
        Returns:
            Dictionary with execution statistics.
        
        Raises:
            ValueError: If source or sink is not set.
            Exception: If on_error='raise' and an error occurs.
        """
        if not self._source:
            raise ValueError("Pipeline source is required")
        if not self._sink:
            raise ValueError("Pipeline sink is required")
        
        logger.info(f"Starting pipeline: {self.name}")
        start_time = time.time()
        
        self._row_count = 0
        self._chunk_count = 0
        self._error_count = 0
        
        try:
            # Extract
            stream = self._source.extract()
            
            # Transform (lazy)
            for transformer in self._transformers:
                stream = self._apply_transformer(stream, transformer)
            
            # Monitor and validate
            stream = self._monitor_stream(stream)
            
            if self.validate_schema and self.schema:
                stream = self._validate_stream(stream)
            
            # Load
            self._sink.load(stream)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        
        duration = time.time() - start_time
        
        stats = {
            "name": self.name,
            "rows": self._row_count,
            "chunks": self._chunk_count,
            "errors": self._error_count,
            "duration": duration,
            "rows_per_second": self._row_count / duration if duration > 0 else 0,
        }
        
        logger.info(
            f"Pipeline complete: {stats['rows']} rows, "
            f"{stats['chunks']} chunks, {stats['duration']:.2f}s"
        )
        
        return stats
    
    def _apply_transformer(
        self,
        stream: Iterable[pd.DataFrame],
        transformer: Transformer,
    ) -> Generator[pd.DataFrame, None, None]:
        """Apply transformer to stream with error handling."""
        transformer_name = type(transformer).__name__
        
        for chunk in stream:
            if chunk is None or chunk.empty:
                continue
            
            try:
                result = transformer.transform(chunk)
                
                if result is not None and not result.empty:
                    yield result
                    
            except Exception as e:
                self._error_count += 1
                
                if self._on_error:
                    self._on_error(e, self._chunk_count)
                
                if self.on_error == "raise":
                    logger.error(f"Transformer {transformer_name} failed: {e}")
                    raise
                elif self.on_error == "log":
                    logger.error(f"Transformer {transformer_name} failed: {e}")
                # If "skip", silently skip this chunk
    
    def _monitor_stream(
        self,
        stream: Iterable[pd.DataFrame],
    ) -> Generator[pd.DataFrame, None, None]:
        """Monitor stream with optional progress bar."""
        if self.show_progress:
            # Wrap with tqdm progress bar
            pbar = tqdm(
                desc=f"Processing {self.name}",
                unit=" rows",
                unit_scale=True,
                dynamic_ncols=True,
            )
        else:
            pbar = None
        
        try:
            for chunk in stream:
                if chunk is None or chunk.empty:
                    continue
                
                chunk_size = len(chunk)
                self._chunk_count += 1
                
                if self._on_chunk_start:
                    self._on_chunk_start(self._chunk_count)
                
                self._row_count += chunk_size
                
                if pbar:
                    pbar.update(chunk_size)
                
                yield chunk
                
                if self._on_chunk_complete:
                    self._on_chunk_complete(self._chunk_count, chunk_size)
        finally:
            if pbar:
                pbar.close()
    
    def _validate_stream(
        self,
        stream: Iterable[pd.DataFrame],
    ) -> Generator[pd.DataFrame, None, None]:
        """Validate chunks against schema."""
        validator = SchemaValidator(
            self.schema,
            coerce=True,
            on_error="warn" if self.on_error != "raise" else "raise",
        )
        
        for chunk in stream:
            validated = validator.validate(chunk)
            yield validated
    
    def clear(self) -> "Pipeline":
        """Clear all components and reset state."""
        self._source = None
        self._transformers = []
        self._sink = None
        self._row_count = 0
        self._chunk_count = 0
        self._error_count = 0
        return self
    
    def copy(self) -> "Pipeline":
        """Create a copy of this pipeline."""
        new_pipeline = Pipeline(
            name=self.name,
            show_progress=self.show_progress,
            validate_schema=self.validate_schema,
            schema=self.schema,
            on_error=self.on_error,
        )
        new_pipeline._source = self._source
        new_pipeline._transformers = list(self._transformers)
        new_pipeline._sink = self._sink
        return new_pipeline
    
    def __repr__(self) -> str:
        components = [
            f"source={type(self._source).__name__}" if self._source else "source=None",
            f"transformers={len(self._transformers)}",
            f"sink={type(self._sink).__name__}" if self._sink else "sink=None",
        ]
        return f"Pipeline({self.name}, {', '.join(components)})"


class PipelineBuilder:
    """
    Fluent builder for creating pipelines from configuration.
    
    Example:
        pipeline = (
            PipelineBuilder("my_pipeline")
            .from_csv("input.csv")
            .filter(lambda df: df['value'] > 100)
            .rename({'value': 'amount'})
            .to_parquet("output.parquet")
            .build()
        )
        
        pipeline.run()
    """
    
    def __init__(self, name: str = "pipeline"):
        self._pipeline = Pipeline(name=name)
    
    def from_csv(self, filepath: str, **kwargs) -> "PipelineBuilder":
        """Set CSV source."""
        from mini_etl.components.extractors import CSVExtractor
        self._pipeline.set_source(CSVExtractor(filepath, **kwargs))
        return self
    
    def from_json(self, filepath: str, **kwargs) -> "PipelineBuilder":
        """Set JSON source."""
        from mini_etl.components.extractors import JSONExtractor
        self._pipeline.set_source(JSONExtractor(filepath, **kwargs))
        return self
    
    def from_sql(self, connection_string: str, **kwargs) -> "PipelineBuilder":
        """Set SQL source."""
        from mini_etl.components.extractors import SQLExtractor
        self._pipeline.set_source(SQLExtractor(connection_string, **kwargs))
        return self
    
    def filter(self, condition) -> "PipelineBuilder":
        """Add filter transformer."""
        from mini_etl.components.transformers import FilterTransformer
        self._pipeline.add_transformer(FilterTransformer(condition))
        return self
    
    def rename(self, columns: dict) -> "PipelineBuilder":
        """Add rename transformer."""
        from mini_etl.components.transformers import RenameTransformer
        self._pipeline.add_transformer(RenameTransformer(columns))
        return self
    
    def select(self, columns: list) -> "PipelineBuilder":
        """Add select columns transformer."""
        from mini_etl.components.transformers import SelectColumnsTransformer
        self._pipeline.add_transformer(SelectColumnsTransformer(columns))
        return self
    
    def drop(self, columns: list) -> "PipelineBuilder":
        """Add drop columns transformer."""
        from mini_etl.components.transformers import DropColumnsTransformer
        self._pipeline.add_transformer(DropColumnsTransformer(columns))
        return self
    
    def transform(self, func) -> "PipelineBuilder":
        """Add custom transform function."""
        from mini_etl.components.transformers import LambdaTransformer
        self._pipeline.add_transformer(LambdaTransformer(func))
        return self
    
    def to_csv(self, filepath: str, **kwargs) -> "PipelineBuilder":
        """Set CSV sink."""
        from mini_etl.components.loaders import CSVLoader
        self._pipeline.set_sink(CSVLoader(filepath, **kwargs))
        return self
    
    def to_parquet(self, filepath: str, **kwargs) -> "PipelineBuilder":
        """Set Parquet sink."""
        from mini_etl.components.loaders import ParquetLoader
        self._pipeline.set_sink(ParquetLoader(filepath, **kwargs))
        return self
    
    def to_sql(self, connection_string: str, table_name: str, **kwargs) -> "PipelineBuilder":
        """Set SQL sink."""
        from mini_etl.components.loaders import SQLLoader
        self._pipeline.set_sink(SQLLoader(connection_string, table_name, **kwargs))
        return self
    
    def with_progress(self, show: bool = True) -> "PipelineBuilder":
        """Enable/disable progress bar."""
        self._pipeline.show_progress = show
        return self
    
    def build(self) -> Pipeline:
        """Build and return the pipeline."""
        return self._pipeline
