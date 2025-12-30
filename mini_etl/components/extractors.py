"""
Data extractors for various sources.

All extractors yield DataFrames in chunks for memory-efficient streaming.
"""

from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Union
import logging
import os

import pandas as pd

from mini_etl.core.base import DataSource
from mini_etl.core.retry import retry_with_backoff, RetryConfig

logger = logging.getLogger(__name__)


class CSVExtractor(DataSource):
    """
    Extract data from CSV files with chunked reading.
    
    Args:
        filepath: Path to CSV file.
        chunksize: Number of rows per chunk.
        **kwargs: Additional arguments passed to pd.read_csv.
    
    Example:
        extractor = CSVExtractor("data.csv", chunksize=10000)
        for chunk in extractor.extract():
            print(chunk.shape)
    """
    
    def __init__(
        self, 
        filepath: str, 
        chunksize: int = 10000,
        encoding: str = "utf-8",
        **kwargs,
    ):
        if not filepath:
            raise ValueError("filepath is required")
        
        self.filepath = filepath
        self.chunksize = chunksize
        self.encoding = encoding
        self.kwargs = kwargs
    
    def extract(self) -> Iterable[pd.DataFrame]:
        """Yield chunks from CSV file."""
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"CSV file not found: {self.filepath}")
        
        logger.info(f"Extracting from CSV: {self.filepath}")
        
        try:
            reader = pd.read_csv(
                self.filepath,
                chunksize=self.chunksize,
                encoding=self.encoding,
                **self.kwargs,
            )
            
            for i, chunk in enumerate(reader):
                if chunk.empty:
                    logger.debug(f"Skipping empty chunk {i}")
                    continue
                logger.debug(f"Yielding chunk {i} with {len(chunk)} rows")
                yield chunk
                
        except pd.errors.EmptyDataError:
            logger.warning(f"CSV file is empty: {self.filepath}")
            return
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            raise


class JSONExtractor(DataSource):
    """
    Extract data from JSON or JSONL files.
    
    Args:
        filepath: Path to JSON file.
        lines: If True, expect JSONL format (one JSON object per line).
        chunksize: Number of rows per chunk (only for lines=True).
        **kwargs: Additional arguments passed to pd.read_json.
    
    Example:
        # JSONL file
        extractor = JSONExtractor("data.jsonl", lines=True, chunksize=5000)
        
        # Regular JSON array
        extractor = JSONExtractor("data.json", lines=False)
    """
    
    def __init__(
        self, 
        filepath: str, 
        lines: bool = False, 
        chunksize: Optional[int] = 10000,
        **kwargs,
    ):
        if not filepath:
            raise ValueError("filepath is required")
        
        self.filepath = filepath
        self.lines = lines
        self.chunksize = chunksize
        self.kwargs = kwargs
    
    def extract(self) -> Iterable[pd.DataFrame]:
        """Yield chunks from JSON file."""
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"JSON file not found: {self.filepath}")
        
        logger.info(f"Extracting from JSON: {self.filepath}")
        
        try:
            if self.lines and self.chunksize:
                # JSONL with chunking
                reader = pd.read_json(
                    self.filepath,
                    lines=True,
                    chunksize=self.chunksize,
                    **self.kwargs,
                )
                for chunk in reader:
                    if not chunk.empty:
                        yield chunk
            else:
                # Regular JSON or JSONL without chunking
                df = pd.read_json(
                    self.filepath,
                    lines=self.lines,
                    **self.kwargs,
                )
                if not df.empty:
                    yield df
                    
        except ValueError as e:
            if "empty" in str(e).lower():
                logger.warning(f"JSON file is empty: {self.filepath}")
                return
            raise
        except Exception as e:
            logger.error(f"Error reading JSON: {e}")
            raise


class ExcelExtractor(DataSource):
    """
    Extract data from Excel files (.xlsx, .xls).
    
    Args:
        filepath: Path to Excel file.
        sheet_name: Sheet to read (name or index). Default is first sheet.
        chunksize: Number of rows per chunk (simulated via skiprows/nrows).
        **kwargs: Additional arguments passed to pd.read_excel.
    
    Example:
        extractor = ExcelExtractor("data.xlsx", sheet_name="Sales")
        for chunk in extractor.extract():
            print(chunk.shape)
    """
    
    def __init__(
        self, 
        filepath: str,
        sheet_name: Union[str, int] = 0,
        chunksize: int = 10000,
        **kwargs,
    ):
        if not filepath:
            raise ValueError("filepath is required")
        
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.chunksize = chunksize
        self.kwargs = kwargs
    
    def extract(self) -> Iterable[pd.DataFrame]:
        """Yield chunks from Excel file."""
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Excel file not found: {self.filepath}")
        
        logger.info(f"Extracting from Excel: {self.filepath}")
        
        try:
            # First, read just to get header
            header_df = pd.read_excel(
                self.filepath,
                sheet_name=self.sheet_name,
                nrows=0,
                **self.kwargs,
            )
            columns = header_df.columns.tolist()
            
            # Get total row count (expensive for large files but necessary for chunking)
            full_df = pd.read_excel(
                self.filepath,
                sheet_name=self.sheet_name,
                **self.kwargs,
            )
            total_rows = len(full_df)
            
            if total_rows == 0:
                logger.warning(f"Excel file is empty: {self.filepath}")
                return
            
            # Yield in chunks
            for start_row in range(0, total_rows, self.chunksize):
                end_row = min(start_row + self.chunksize, total_rows)
                chunk = full_df.iloc[start_row:end_row].copy()
                logger.debug(f"Yielding rows {start_row}-{end_row}")
                yield chunk
                
        except Exception as e:
            logger.error(f"Error reading Excel: {e}")
            raise


class ParquetExtractor(DataSource):
    """
    Extract data from Parquet files.
    
    Args:
        filepath: Path to Parquet file or directory of Parquet files.
        columns: Optional list of columns to read (column pruning).
        chunksize: Number of rows per chunk.
        **kwargs: Additional arguments passed to pd.read_parquet.
    
    Example:
        extractor = ParquetExtractor(
            "data.parquet", 
            columns=["id", "value", "timestamp"]
        )
    """
    
    def __init__(
        self, 
        filepath: str,
        columns: Optional[List[str]] = None,
        chunksize: int = 10000,
        **kwargs,
    ):
        if not filepath:
            raise ValueError("filepath is required")
        
        self.filepath = filepath
        self.columns = columns
        self.chunksize = chunksize
        self.kwargs = kwargs
    
    def extract(self) -> Iterable[pd.DataFrame]:
        """Yield chunks from Parquet file."""
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Parquet file not found: {self.filepath}")
        
        logger.info(f"Extracting from Parquet: {self.filepath}")
        
        try:
            # Read the full file (Parquet is already columnar and efficient)
            df = pd.read_parquet(
                self.filepath,
                columns=self.columns,
                **self.kwargs,
            )
            
            if df.empty:
                logger.warning(f"Parquet file is empty: {self.filepath}")
                return
            
            # Yield in chunks
            for start_row in range(0, len(df), self.chunksize):
                end_row = min(start_row + self.chunksize, len(df))
                chunk = df.iloc[start_row:end_row].copy()
                yield chunk
                
        except Exception as e:
            logger.error(f"Error reading Parquet: {e}")
            raise


class SQLExtractor(DataSource):
    """
    Extract data from SQL databases.
    
    Args:
        connection_string: SQLAlchemy connection string.
        query: SQL query to execute (mutually exclusive with table).
        table: Table name to read (mutually exclusive with query).
        chunksize: Number of rows per chunk.
        **kwargs: Additional arguments passed to pd.read_sql.
    
    Example:
        # Using a query
        extractor = SQLExtractor(
            "postgresql://user:pass@host/db",
            query="SELECT * FROM users WHERE active = true"
        )
        
        # Using a table name
        extractor = SQLExtractor(
            "sqlite:///data.db",
            table="users"
        )
    """
    
    def __init__(
        self,
        connection_string: str,
        query: Optional[str] = None,
        table: Optional[str] = None,
        chunksize: int = 10000,
        **kwargs,
    ):
        if not connection_string:
            raise ValueError("connection_string is required")
        if not query and not table:
            raise ValueError("Either query or table is required")
        if query and table:
            raise ValueError("Specify either query or table, not both")
        
        self.connection_string = connection_string
        self.query = query
        self.table = table
        self.chunksize = chunksize
        self.kwargs = kwargs
    
    def extract(self) -> Iterable[pd.DataFrame]:
        """Yield chunks from SQL database."""
        from sqlalchemy import create_engine, text
        
        logger.info(f"Extracting from SQL: {self.table or 'query'}")
        
        try:
            engine = create_engine(self.connection_string)
            
            sql = self.query if self.query else f"SELECT * FROM {self.table}"
            
            with engine.connect() as conn:
                reader = pd.read_sql(
                    text(sql),
                    conn,
                    chunksize=self.chunksize,
                    **self.kwargs,
                )
                
                for chunk in reader:
                    if not chunk.empty:
                        yield chunk
                        
        except Exception as e:
            logger.error(f"Error reading from SQL: {e}")
            raise


class APIExtractor(DataSource):
    """
    Extract data from REST APIs with pagination support.
    
    Args:
        url: API endpoint URL.
        method: HTTP method (GET or POST).
        headers: Request headers.
        params: Query parameters.
        data_path: JSON path to data array (e.g., "data", "results.items").
        pagination: Pagination configuration.
        auth: Authentication tuple (username, password) or bearer token.
        timeout: Request timeout in seconds.
        retry_config: Retry configuration.
    
    Example:
        extractor = APIExtractor(
            "https://api.example.com/users",
            headers={"Authorization": "Bearer token"},
            data_path="data",
            pagination={
                "type": "page",
                "page_param": "page",
                "limit_param": "limit",
                "limit": 100,
            }
        )
    """
    
    def __init__(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        data_path: Optional[str] = None,
        pagination: Optional[Dict[str, Any]] = None,
        auth: Optional[Union[tuple, str]] = None,
        timeout: int = 30,
        retry_config: Optional[RetryConfig] = None,
    ):
        if not url:
            raise ValueError("url is required")
        
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}
        self.params = params or {}
        self.body = body
        self.data_path = data_path
        self.pagination = pagination or {}
        self.auth = auth
        self.timeout = timeout
        self.retry_config = retry_config or RetryConfig(
            max_attempts=3,
            retry_exceptions=(ConnectionError, TimeoutError, OSError),
        )
    
    def extract(self) -> Iterable[pd.DataFrame]:
        """Yield chunks from API."""
        import requests
        
        logger.info(f"Extracting from API: {self.url}")
        
        session = requests.Session()
        session.headers.update(self.headers)
        
        # Handle authentication
        if isinstance(self.auth, tuple):
            session.auth = self.auth
        elif isinstance(self.auth, str):
            session.headers["Authorization"] = f"Bearer {self.auth}"
        
        page = 1
        has_more = True
        
        while has_more:
            try:
                data = self._fetch_page(session, page)
                
                if not data:
                    has_more = False
                    continue
                
                df = pd.DataFrame(data)
                
                if df.empty:
                    has_more = False
                    continue
                
                yield df
                
                # Check pagination
                if self.pagination:
                    page += 1
                    limit = self.pagination.get("limit", 100)
                    if len(data) < limit:
                        has_more = False
                else:
                    has_more = False
                    
            except Exception as e:
                logger.error(f"Error fetching from API: {e}")
                raise
    
    @retry_with_backoff()
    def _fetch_page(self, session, page: int) -> List[Dict]:
        """Fetch a single page of data."""
        import requests
        
        params = dict(self.params)
        
        # Add pagination params
        if self.pagination:
            ptype = self.pagination.get("type", "page")
            
            if ptype == "page":
                page_param = self.pagination.get("page_param", "page")
                limit_param = self.pagination.get("limit_param", "limit")
                limit = self.pagination.get("limit", 100)
                
                params[page_param] = page
                params[limit_param] = limit
                
            elif ptype == "offset":
                offset_param = self.pagination.get("offset_param", "offset")
                limit_param = self.pagination.get("limit_param", "limit")
                limit = self.pagination.get("limit", 100)
                
                params[offset_param] = (page - 1) * limit
                params[limit_param] = limit
        
        # Make request
        if self.method == "GET":
            response = session.get(
                self.url,
                params=params,
                timeout=self.timeout,
            )
        else:
            response = session.post(
                self.url,
                params=params,
                json=self.body,
                timeout=self.timeout,
            )
        
        response.raise_for_status()
        result = response.json()
        
        # Extract data using path
        if self.data_path:
            for key in self.data_path.split("."):
                if isinstance(result, dict):
                    result = result.get(key, [])
                else:
                    break
        
        if isinstance(result, list):
            return result
        elif isinstance(result, dict):
            return [result]
        else:
            return []


class InMemoryExtractor(DataSource):
    """
    Extract data from in-memory DataFrame or list of DataFrames.
    
    Useful for testing or when data is already in memory.
    
    Args:
        data: Single DataFrame or list of DataFrames.
        chunksize: If single DataFrame, split into chunks of this size.
    
    Example:
        df = pd.DataFrame({"a": [1, 2, 3]})
        extractor = InMemoryExtractor(df)
    """
    
    def __init__(
        self,
        data: Union[pd.DataFrame, List[pd.DataFrame]],
        chunksize: Optional[int] = None,
    ):
        if isinstance(data, pd.DataFrame):
            self.data = [data]
        else:
            self.data = data
        self.chunksize = chunksize
    
    def extract(self) -> Iterable[pd.DataFrame]:
        """Yield chunks from memory."""
        for df in self.data:
            if self.chunksize and len(df) > self.chunksize:
                for start in range(0, len(df), self.chunksize):
                    end = min(start + self.chunksize, len(df))
                    yield df.iloc[start:end].copy()
            else:
                yield df
