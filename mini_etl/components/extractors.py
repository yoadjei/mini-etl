from typing import Iterable, Optional
import pandas as pd
from ..core.base import DataSource

class CSVExtractor(DataSource):
    def __init__(self, filepath: str, chunksize: int = 10000, **kwargs):
        self.filepath = filepath
        self.chunksize = chunksize
        self.kwargs = kwargs

    def extract(self) -> Iterable[pd.DataFrame]:
        # Yields chunks. Good for big files.
        return pd.read_csv(self.filepath, chunksize=self.chunksize, **self.kwargs)

class JSONExtractor(DataSource):
    def __init__(self, filepath: str, lines: bool = False, chunksize: Optional[int] = 10000, **kwargs):
        self.filepath = filepath
        self.lines = lines
        self.chunksize = chunksize
        self.kwargs = kwargs

    def extract(self) -> Iterable[pd.DataFrame]:
        # pd.read_json needs lines=True to support chunking
        if self.lines and self.chunksize:
             return pd.read_json(self.filepath, lines=True, chunksize=self.chunksize, **self.kwargs)
        
        # Otherwise, just eat the whole file
        yield pd.read_json(self.filepath, lines=self.lines, **self.kwargs)
