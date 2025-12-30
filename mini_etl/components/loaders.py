"""
Data loaders for various destinations.

All loaders consume chunk streams for memory-efficient streaming.
"""

import os
from typing import Any, Dict, Iterable, List, Optional
import logging

import pandas as pd

from mini_etl.core.base import DataSink

logger = logging.getLogger(__name__)


class CSVLoader(DataSink):
    """
    Load data to CSV files.
    
    Args:
        filepath: Output file path.
        mode: Write mode - 'w' (overwrite) or 'a' (append).
        encoding: File encoding.
        **kwargs: Additional arguments passed to DataFrame.to_csv.
    
    Example:
        loader = CSVLoader("output.csv", mode='w')
        loader.load(chunk_stream)
    """
    
    def __init__(
        self, 
        filepath: str, 
        mode: str = "w",
        encoding: str = "utf-8",
        **kwargs,
    ):
        if not filepath:
            raise ValueError("filepath is required")
        if mode not in ("w", "a"):
            raise ValueError("mode must be 'w' or 'a'")
        
        self.filepath = filepath
        self.mode = mode
        self.encoding = encoding
        self.kwargs = kwargs
    
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        """Write chunks to CSV file."""
        logger.info(f"Loading to CSV: {self.filepath}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.filepath) or ".", exist_ok=True)
        
        is_first_chunk = True
        current_mode = self.mode
        rows_written = 0
        
        for chunk in data:
            if chunk is None or chunk.empty:
                continue
            
            write_header = False
            
            if is_first_chunk:
                if self.mode == "w":
                    write_header = True
                elif self.mode == "a":
                    # Only write header if file doesn't exist or is empty
                    write_header = (
                        not os.path.exists(self.filepath) or 
                        os.path.getsize(self.filepath) == 0
                    )
                
                chunk.to_csv(
                    self.filepath,
                    mode=current_mode,
                    header=write_header,
                    index=False,
                    encoding=self.encoding,
                    **self.kwargs,
                )
                current_mode = "a"
                is_first_chunk = False
            else:
                chunk.to_csv(
                    self.filepath,
                    mode="a",
                    header=False,
                    index=False,
                    encoding=self.encoding,
                    **self.kwargs,
                )
            
            rows_written += len(chunk)
        
        logger.info(f"CSV load complete: {rows_written} rows written")


class JSONLoader(DataSink):
    """
    Load data to JSON/JSONL files.
    
    Args:
        filepath: Output file path.
        lines: If True, write as JSONL (one JSON object per line).
        mode: Write mode - 'w' (overwrite) or 'a' (append).
        **kwargs: Additional arguments passed to DataFrame.to_json.
    
    Example:
        # JSONL (streaming-friendly)
        loader = JSONLoader("output.jsonl", lines=True)
        
        # Regular JSON array (not recommended for large data)
        loader = JSONLoader("output.json", lines=False)
    """
    
    def __init__(
        self, 
        filepath: str, 
        lines: bool = True, 
        mode: str = "w",
        **kwargs,
    ):
        if not filepath:
            raise ValueError("filepath is required")
        if not lines:
            logger.warning(
                "lines=False requires buffering all data in memory. "
                "Consider using lines=True for streaming."
            )
        
        self.filepath = filepath
        self.lines = lines
        self.mode = mode
        self.kwargs = kwargs
    
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        """Write chunks to JSON file."""
        logger.info(f"Loading to JSON: {self.filepath}")
        
        os.makedirs(os.path.dirname(self.filepath) or ".", exist_ok=True)
        
        if self.lines:
            self._load_jsonl(data)
        else:
            self._load_json_array(data)
    
    def _load_jsonl(self, data: Iterable[pd.DataFrame]) -> None:
        """Write as JSONL (streaming)."""
        is_first_chunk = True
        rows_written = 0
        
        for chunk in data:
            if chunk is None or chunk.empty:
                continue
            
            run_mode = "w" if (is_first_chunk and self.mode == "w") else "a"
            
            chunk.to_json(
                self.filepath,
                orient="records",
                lines=True,
                mode=run_mode,
                **self.kwargs,
            )
            
            is_first_chunk = False
            rows_written += len(chunk)
        
        logger.info(f"JSONL load complete: {rows_written} rows written")
    
    def _load_json_array(self, data: Iterable[pd.DataFrame]) -> None:
        """Write as JSON array (requires buffering)."""
        all_chunks = []
        
        for chunk in data:
            if chunk is not None and not chunk.empty:
                all_chunks.append(chunk)
        
        if not all_chunks:
            logger.warning("No data to write")
            return
        
        combined = pd.concat(all_chunks, ignore_index=True)
        combined.to_json(
            self.filepath,
            orient="records",
            **self.kwargs,
        )
        
        logger.info(f"JSON load complete: {len(combined)} rows written")


class SQLLoader(DataSink):
    """
    Load data to SQL databases.
    
    Args:
        connection_string: SQLAlchemy connection string.
        table_name: Target table name.
        if_exists: Behavior if table exists - 'fail', 'replace', 'append'.
        schema: Optional database schema.
        **kwargs: Additional arguments passed to DataFrame.to_sql.
    
    Example:
        loader = SQLLoader(
            "postgresql://user:pass@host/db",
            "users",
            if_exists="append"
        )
    """
    
    def __init__(
        self, 
        connection_string: str, 
        table_name: str, 
        if_exists: str = "append",
        schema: Optional[str] = None,
        chunksize: int = 1000,
        **kwargs,
    ):
        if not connection_string:
            raise ValueError("connection_string is required")
        if not table_name:
            raise ValueError("table_name is required")
        if if_exists not in ("fail", "replace", "append"):
            raise ValueError("if_exists must be 'fail', 'replace', or 'append'")
        
        self.connection_string = connection_string
        self.table_name = table_name
        self.if_exists = if_exists
        self.schema = schema
        self.chunksize = chunksize
        self.kwargs = kwargs
    
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        """Write chunks to SQL database."""
        from sqlalchemy import create_engine
        
        logger.info(f"Loading to SQL table: {self.table_name}")
        
        engine = create_engine(self.connection_string)
        current_if_exists = self.if_exists
        rows_written = 0
        
        try:
            for chunk in data:
                if chunk is None or chunk.empty:
                    continue
                
                chunk.to_sql(
                    self.table_name,
                    engine,
                    if_exists=current_if_exists,
                    index=False,
                    schema=self.schema,
                    chunksize=self.chunksize,
                    **self.kwargs,
                )
                
                # After first chunk, switch to append
                if current_if_exists == "replace":
                    current_if_exists = "append"
                
                rows_written += len(chunk)
            
            logger.info(f"SQL load complete: {rows_written} rows written")
            
        except Exception as e:
            logger.error(f"SQL load error: {e}")
            raise
        finally:
            engine.dispose()


class ParquetLoader(DataSink):
    """
    Load data to Parquet files.
    
    Args:
        filepath: Output file path.
        mode: Write mode - 'overwrite' or 'append'.
        engine: Parquet engine ('pyarrow' or 'fastparquet').
        compression: Compression codec ('snappy', 'gzip', 'brotli', None).
        **kwargs: Additional arguments passed to DataFrame.to_parquet.
    
    Example:
        loader = ParquetLoader("output.parquet", compression="snappy")
    
    Note: For streaming to Parquet, this loader either:
    - Buffers all data for a single file (default for small-medium data)
    - Writes partition files for large data (when partition_cols specified)
    """
    
    def __init__(
        self, 
        filepath: str, 
        mode: str = "overwrite",
        engine: str = "pyarrow",
        compression: str = "snappy",
        partition_cols: Optional[List[str]] = None,
        **kwargs,
    ):
        if not filepath:
            raise ValueError("filepath is required")
        
        self.filepath = filepath
        self.mode = mode
        self.engine = engine
        self.compression = compression
        self.partition_cols = partition_cols
        self.kwargs = kwargs
    
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        """Write chunks to Parquet file."""
        logger.info(f"Loading to Parquet: {self.filepath}")
        
        os.makedirs(os.path.dirname(self.filepath) or ".", exist_ok=True)
        
        if self.partition_cols:
            self._load_partitioned(data)
        else:
            self._load_single_file(data)
    
    def _load_single_file(self, data: Iterable[pd.DataFrame]) -> None:
        """Write all data to a single Parquet file."""
        all_chunks = []
        
        for chunk in data:
            if chunk is not None and not chunk.empty:
                all_chunks.append(chunk)
        
        if not all_chunks:
            logger.warning("No data to write")
            return
        
        combined = pd.concat(all_chunks, ignore_index=True)
        
        combined.to_parquet(
            self.filepath,
            engine=self.engine,
            compression=self.compression,
            index=False,
            **self.kwargs,
        )
        
        logger.info(f"Parquet load complete: {len(combined)} rows written")
    
    def _load_partitioned(self, data: Iterable[pd.DataFrame]) -> None:
        """Write data with partitioning."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        # Create output directory
        os.makedirs(self.filepath, exist_ok=True)
        
        rows_written = 0
        
        for i, chunk in enumerate(data):
            if chunk is None or chunk.empty:
                continue
            
            table = pa.Table.from_pandas(chunk)
            
            pq.write_to_dataset(
                table,
                root_path=self.filepath,
                partition_cols=self.partition_cols,
                compression=self.compression,
                **self.kwargs,
            )
            
            rows_written += len(chunk)
        
        logger.info(f"Partitioned Parquet load complete: {rows_written} rows written")


class ExcelLoader(DataSink):
    """
    Load data to Excel files.
    
    Args:
        filepath: Output file path (.xlsx).
        sheet_name: Target sheet name.
        mode: Write mode - 'w' (overwrite) or 'a' (append to existing).
        **kwargs: Additional arguments passed to DataFrame.to_excel.
    
    Example:
        loader = ExcelLoader("output.xlsx", sheet_name="Data")
    
    Note: Excel writing requires buffering all data in memory.
    For large datasets, consider using CSV or Parquet instead.
    """
    
    def __init__(
        self, 
        filepath: str, 
        sheet_name: str = "Sheet1",
        mode: str = "w",
        **kwargs,
    ):
        if not filepath:
            raise ValueError("filepath is required")
        if not filepath.endswith((".xlsx", ".xls")):
            logger.warning("Excel files should have .xlsx or .xls extension")
        
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.mode = mode
        self.kwargs = kwargs
    
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        """Write chunks to Excel file."""
        logger.info(f"Loading to Excel: {self.filepath}")
        
        os.makedirs(os.path.dirname(self.filepath) or ".", exist_ok=True)
        
        # Buffer all data (Excel doesn't support streaming writes well)
        all_chunks = []
        
        for chunk in data:
            if chunk is not None and not chunk.empty:
                all_chunks.append(chunk)
        
        if not all_chunks:
            logger.warning("No data to write")
            return
        
        combined = pd.concat(all_chunks, ignore_index=True)
        
        if self.mode == "a" and os.path.exists(self.filepath):
            # Append to existing file
            try:
                existing = pd.read_excel(self.filepath, sheet_name=self.sheet_name)
                combined = pd.concat([existing, combined], ignore_index=True)
            except Exception:
                # Sheet doesn't exist, just write new data
                pass
        
        combined.to_excel(
            self.filepath,
            sheet_name=self.sheet_name,
            index=False,
            **self.kwargs,
        )
        
        logger.info(f"Excel load complete: {len(combined)} rows written")


class MultiLoader(DataSink):
    """
    Load data to multiple destinations simultaneously.
    
    Args:
        loaders: List of DataSink instances to write to.
    
    Example:
        loader = MultiLoader([
            CSVLoader("output.csv"),
            SQLLoader("sqlite:///data.db", "table"),
        ])
    """
    
    def __init__(self, loaders: List[DataSink]):
        if not loaders:
            raise ValueError("At least one loader is required")
        self.loaders = loaders
    
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        """Write chunks to all destinations."""
        logger.info(f"Loading to {len(self.loaders)} destinations")
        
        # Buffer chunks since we need to iterate multiple times
        chunks = list(data)
        
        for loader in self.loaders:
            try:
                loader.load(iter(chunks))
            except Exception as e:
                logger.error(f"Loader {type(loader).__name__} failed: {e}")
                raise


class NullLoader(DataSink):
    """
    Discards all data. Useful for testing or dry runs.
    
    Example:
        loader = NullLoader()  # Data goes nowhere
    """
    
    def __init__(self, log_stats: bool = True):
        self.log_stats = log_stats
    
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        """Consume and discard all data."""
        rows = 0
        chunks = 0
        
        for chunk in data:
            if chunk is not None and not chunk.empty:
                rows += len(chunk)
                chunks += 1
        
        if self.log_stats:
            logger.info(f"NullLoader consumed {rows} rows in {chunks} chunks")


class CallbackLoader(DataSink):
    """
    Call a function for each chunk. Useful for custom processing.
    
    Args:
        callback: Function called with each DataFrame chunk.
        on_complete: Optional function called after all chunks processed.
    
    Example:
        def process_chunk(df):
            print(f"Processing {len(df)} rows")
        
        loader = CallbackLoader(process_chunk)
    """
    
    def __init__(
        self,
        callback: callable,
        on_complete: Optional[callable] = None,
    ):
        if not callable(callback):
            raise ValueError("callback must be callable")
        
        self.callback = callback
        self.on_complete = on_complete
    
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        """Call callback for each chunk."""
        for chunk in data:
            if chunk is not None and not chunk.empty:
                self.callback(chunk)
        
        if self.on_complete:
            self.on_complete()
