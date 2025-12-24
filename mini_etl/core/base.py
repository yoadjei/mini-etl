from abc import ABC, abstractmethod
from typing import Iterable
import pandas as pd

class DataSource(ABC):
    @abstractmethod
    def extract(self) -> Iterable[pd.DataFrame]:
        # Yield dataframes. Using generators works best for big data.
        pass

class Transformer(ABC):
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

class DataSink(ABC):
    @abstractmethod
    def load(self, data: Iterable[pd.DataFrame]) -> None:
        # Consumes the stream and writes it out.
        pass
