from typing import Callable, Dict, List
import pandas as pd
from ..core.base import Transformer

class FilterTransformer(Transformer):
    def __init__(self, condition: Callable[[pd.DataFrame], pd.Series]):
        # condition: function returning a boolean mask (e.g. df['age'] > 20)
        self.condition = condition

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data[self.condition(data)]

class RenameTransformer(Transformer):
    def __init__(self, columns: Dict[str, str]):
        self.columns = columns

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.rename(columns=self.columns)

class GroupAggTransformer(Transformer):
    def __init__(self, group_by: List[str], agg: Dict[str, str]):
        self.group_by = group_by
        self.agg = agg

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        # Heads up: this aggregates PER CHUNK. 
        # For global stats, we'd need a stateful transformer.
        return data.groupby(self.group_by).agg(self.agg).reset_index()
