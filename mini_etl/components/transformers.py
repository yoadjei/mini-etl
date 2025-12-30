"""
Data transformers for ETL pipelines.

All transformers implement the Transformer interface and can be chained.
"""

from typing import Any, Callable, Dict, List, Optional, Union
import logging
import re

import pandas as pd
import numpy as np

from mini_etl.core.base import Transformer

logger = logging.getLogger(__name__)


class FilterTransformer(Transformer):
    """
    Filter rows based on a condition.
    
    Args:
        condition: Function that takes a DataFrame and returns a boolean Series.
    
    Example:
        # Filter where value > 100
        transformer = FilterTransformer(lambda df: df['value'] > 100)
        
        # Multiple conditions
        transformer = FilterTransformer(
            lambda df: (df['value'] > 100) & (df['status'] == 'active')
        )
    """
    
    def __init__(self, condition: Callable[[pd.DataFrame], pd.Series]):
        if not callable(condition):
            raise ValueError("condition must be callable")
        self.condition = condition
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply filter condition to data."""
        if data.empty:
            return data
        
        try:
            mask = self.condition(data)
            return data[mask].copy()
        except Exception as e:
            logger.error(f"Filter error: {e}")
            raise ValueError(f"Filter condition failed: {e}") from e


class RenameTransformer(Transformer):
    """
    Rename columns.
    
    Args:
        columns: Dictionary mapping old names to new names.
    
    Example:
        transformer = RenameTransformer({
            'old_name': 'new_name',
            'another_old': 'another_new'
        })
    """
    
    def __init__(self, columns: Dict[str, str]):
        if not isinstance(columns, dict):
            raise ValueError("columns must be a dictionary")
        self.columns = columns
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Rename columns in data."""
        if data.empty:
            return data
        
        # Only rename columns that exist
        existing_renames = {
            old: new for old, new in self.columns.items() 
            if old in data.columns
        }
        
        missing = set(self.columns.keys()) - set(existing_renames.keys())
        if missing:
            logger.warning(f"Columns not found for renaming: {missing}")
        
        return data.rename(columns=existing_renames)


class SelectColumnsTransformer(Transformer):
    """
    Select specific columns, dropping others.
    
    Args:
        columns: List of column names to keep.
        ignore_missing: If True, ignore columns that don't exist.
    
    Example:
        transformer = SelectColumnsTransformer(['id', 'name', 'value'])
    """
    
    def __init__(self, columns: List[str], ignore_missing: bool = True):
        if not isinstance(columns, list):
            raise ValueError("columns must be a list")
        self.columns = columns
        self.ignore_missing = ignore_missing
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Select columns from data."""
        if data.empty:
            return data
        
        # Filter to existing columns
        existing = [col for col in self.columns if col in data.columns]
        missing = set(self.columns) - set(existing)
        
        if missing:
            if self.ignore_missing:
                logger.warning(f"Columns not found: {missing}")
            else:
                raise ValueError(f"Columns not found: {missing}")
        
        if not existing:
            logger.warning("No valid columns to select")
            return pd.DataFrame()
        
        return data[existing].copy()


class DropColumnsTransformer(Transformer):
    """
    Drop specific columns.
    
    Args:
        columns: List of column names to drop.
        ignore_missing: If True, ignore columns that don't exist.
    
    Example:
        transformer = DropColumnsTransformer(['temp_col', 'debug_col'])
    """
    
    def __init__(self, columns: List[str], ignore_missing: bool = True):
        if not isinstance(columns, list):
            raise ValueError("columns must be a list")
        self.columns = columns
        self.ignore_missing = ignore_missing
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Drop columns from data."""
        if data.empty:
            return data
        
        existing = [col for col in self.columns if col in data.columns]
        missing = set(self.columns) - set(existing)
        
        if missing and not self.ignore_missing:
            raise ValueError(f"Columns not found: {missing}")
        
        return data.drop(columns=existing, errors="ignore")


class CastTypeTransformer(Transformer):
    """
    Cast columns to specified types.
    
    Args:
        columns: Dictionary mapping column names to target types.
                 Types can be: 'int', 'float', 'str', 'bool', 'datetime', 
                               or pandas dtype strings.
    
    Example:
        transformer = CastTypeTransformer({
            'id': 'int',
            'price': 'float',
            'created_at': 'datetime'
        })
    """
    
    TYPE_MAP = {
        "int": "Int64",  # Nullable integer
        "float": "float64",
        "str": "object",
        "string": "object",
        "bool": "bool",
        "boolean": "bool",
        "datetime": "datetime64[ns]",
        "date": "datetime64[ns]",
    }
    
    def __init__(self, columns: Dict[str, str]):
        if not isinstance(columns, dict):
            raise ValueError("columns must be a dictionary")
        self.columns = columns
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Cast column types."""
        if data.empty:
            return data
        
        data = data.copy()
        
        for col, dtype in self.columns.items():
            if col not in data.columns:
                logger.warning(f"Column '{col}' not found for type casting")
                continue
            
            target_type = self.TYPE_MAP.get(dtype.lower(), dtype)
            
            try:
                if target_type == "datetime64[ns]":
                    data[col] = pd.to_datetime(data[col], errors="coerce")
                elif target_type == "Int64":
                    data[col] = pd.to_numeric(data[col], errors="coerce").astype("Int64")
                elif target_type in ("float64", "float32"):
                    data[col] = pd.to_numeric(data[col], errors="coerce")
                elif target_type == "bool":
                    data[col] = data[col].astype(bool)
                else:
                    data[col] = data[col].astype(target_type)
            except Exception as e:
                logger.error(f"Failed to cast '{col}' to {dtype}: {e}")
                raise
        
        return data


class FillNATransformer(Transformer):
    """
    Fill missing values.
    
    Args:
        value: Value to fill (scalar or dict mapping columns to values).
        columns: Optional list of columns to fill (if value is scalar).
        method: Fill method ('ffill', 'bfill') - alternative to value.
    
    Example:
        # Fill all nulls with 0
        transformer = FillNATransformer(value=0)
        
        # Fill specific columns
        transformer = FillNATransformer(value={'price': 0, 'name': 'Unknown'})
        
        # Forward fill
        transformer = FillNATransformer(method='ffill')
    """
    
    def __init__(
        self,
        value: Optional[Union[Any, Dict[str, Any]]] = None,
        columns: Optional[List[str]] = None,
        method: Optional[str] = None,
    ):
        if value is None and method is None:
            raise ValueError("Either value or method must be specified")
        if value is not None and method is not None:
            raise ValueError("Specify either value or method, not both")
        
        self.value = value
        self.columns = columns
        self.method = method
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Fill missing values."""
        if data.empty:
            return data
        
        data = data.copy()
        
        if self.method:
            if self.columns:
                data[self.columns] = data[self.columns].fillna(method=self.method)
            else:
                data = data.fillna(method=self.method)
        elif isinstance(self.value, dict):
            data = data.fillna(self.value)
        else:
            if self.columns:
                for col in self.columns:
                    if col in data.columns:
                        data[col] = data[col].fillna(self.value)
            else:
                data = data.fillna(self.value)
        
        return data


class ExpressionTransformer(Transformer):
    """
    Apply pandas expression or create new columns using eval.
    
    Args:
        expression: Pandas eval expression.
        filter_mode: If True, use expression as filter condition.
    
    Example:
        # Create new column
        transformer = ExpressionTransformer("total = price * quantity")
        
        # Filter (alternative to FilterTransformer for string expressions)
        transformer = ExpressionTransformer("price > 100", filter_mode=True)
    """
    
    def __init__(self, expression: str, filter_mode: bool = False):
        if not expression:
            raise ValueError("expression is required")
        self.expression = expression
        self.filter_mode = filter_mode
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply expression to data."""
        if data.empty:
            return data
        
        try:
            if self.filter_mode:
                mask = data.eval(self.expression)
                return data[mask].copy()
            else:
                return data.eval(self.expression)
        except Exception as e:
            logger.error(f"Expression error '{self.expression}': {e}")
            raise ValueError(f"Expression failed: {e}") from e


class GroupAggTransformer(Transformer):
    """
    Group by columns and aggregate.
    
    Note: This performs chunk-wise aggregation. For global aggregation
    across all chunks, use StatefulAggTransformer.
    
    Args:
        group_by: Column(s) to group by.
        agg: Aggregation specification.
    
    Example:
        transformer = GroupAggTransformer(
            group_by=['category', 'region'],
            agg={
                'value': 'sum',
                'count': 'count',
                'price': ['min', 'max', 'mean']
            }
        )
    """
    
    def __init__(
        self,
        group_by: Union[str, List[str]],
        agg: Dict[str, Union[str, List[str]]],
    ):
        if isinstance(group_by, str):
            group_by = [group_by]
        
        self.group_by = group_by
        self.agg = agg
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply groupby aggregation."""
        if data.empty:
            return data
        
        # Check columns exist
        missing = set(self.group_by) - set(data.columns)
        if missing:
            raise ValueError(f"Group by columns not found: {missing}")
        
        result = data.groupby(self.group_by).agg(self.agg).reset_index()
        
        # Flatten multi-level columns if needed
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = [
                "_".join(map(str, col)).strip("_") 
                for col in result.columns.values
            ]
        
        return result


class StatefulAggTransformer(Transformer):
    """
    Stateful aggregation that accumulates across chunks.
    
    This transformer maintains state between chunks to compute
    global aggregations (e.g., overall sum, count, mean).
    
    Args:
        group_by: Column(s) to group by. If None, aggregate all rows.
        agg: Aggregation specification (supports 'sum', 'count', 'min', 'max').
        finalize_on_exhaust: If True, returns accumulated result only at the end.
    
    Example:
        transformer = StatefulAggTransformer(
            group_by=['category'],
            agg={'value': 'sum', 'id': 'count'}
        )
        
        # Process chunks
        for chunk in source:
            result = transformer.transform(chunk)  # Returns empty until finalize
        
        # Get final result
        final = transformer.finalize()
    """
    
    def __init__(
        self,
        group_by: Optional[Union[str, List[str]]] = None,
        agg: Optional[Dict[str, str]] = None,
    ):
        if isinstance(group_by, str):
            group_by = [group_by]
        
        self.group_by = group_by
        self.agg = agg or {}
        self._state: Dict[str, pd.DataFrame] = {}
        self._finalized = False
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Accumulate chunk data into state."""
        if data.empty or self._finalized:
            return pd.DataFrame()
        
        # Compute chunk aggregates
        if self.group_by:
            chunk_agg = data.groupby(self.group_by).agg(self._build_agg_dict()).reset_index()
        else:
            chunk_agg = data.agg(self._build_agg_dict()).to_frame().T
        
        # Merge with existing state
        self._merge_state(chunk_agg)
        
        # Return empty - actual result comes from finalize()
        return pd.DataFrame()
    
    def _build_agg_dict(self) -> Dict[str, Union[str, Callable]]:
        """Build aggregation dict for internal tracking."""
        result = {}
        for col, func in self.agg.items():
            if func in ("sum", "count", "min", "max"):
                result[col] = func
            elif func == "mean":
                # For mean, we need sum and count
                result[f"{col}_sum"] = (col, "sum")
                result[f"{col}_count"] = (col, "count")
            else:
                result[col] = func
        return result
    
    def _merge_state(self, chunk_agg: pd.DataFrame) -> None:
        """Merge chunk aggregates into accumulated state."""
        if not self._state:
            self._state["data"] = chunk_agg
            return
        
        existing = self._state["data"]
        
        if self.group_by:
            # Merge on group keys
            merged = existing.merge(
                chunk_agg,
                on=self.group_by,
                how="outer",
                suffixes=("_old", "_new"),
            )
            
            # Combine aggregates
            for col, func in self.agg.items():
                old_col = f"{col}_old"
                new_col = f"{col}_new"
                
                if old_col in merged.columns and new_col in merged.columns:
                    if func == "sum" or func == "count":
                        merged[col] = merged[old_col].fillna(0) + merged[new_col].fillna(0)
                    elif func == "min":
                        merged[col] = merged[[old_col, new_col]].min(axis=1)
                    elif func == "max":
                        merged[col] = merged[[old_col, new_col]].max(axis=1)
                    
                    merged = merged.drop(columns=[old_col, new_col])
            
            self._state["data"] = merged
        else:
            # Global aggregation
            for col, func in self.agg.items():
                if func == "sum" or func == "count":
                    existing[col] = existing[col].fillna(0) + chunk_agg[col].fillna(0)
                elif func == "min":
                    existing[col] = min(existing[col].iloc[0], chunk_agg[col].iloc[0])
                elif func == "max":
                    existing[col] = max(existing[col].iloc[0], chunk_agg[col].iloc[0])
            
            self._state["data"] = existing
    
    def finalize(self) -> pd.DataFrame:
        """Return the final aggregated result."""
        self._finalized = True
        
        if not self._state:
            return pd.DataFrame()
        
        result = self._state["data"].copy()
        
        # Calculate mean from sum/count if needed
        for col, func in self.agg.items():
            if func == "mean":
                sum_col = f"{col}_sum"
                count_col = f"{col}_count"
                if sum_col in result.columns and count_col in result.columns:
                    result[col] = result[sum_col] / result[count_col]
                    result = result.drop(columns=[sum_col, count_col])
        
        return result
    
    def reset(self) -> None:
        """Reset the transformer state."""
        self._state = {}
        self._finalized = False


class DeduplicateTransformer(Transformer):
    """
    Remove duplicate rows.
    
    Args:
        subset: Columns to consider for duplicates (None = all).
        keep: Which duplicates to keep ('first', 'last', False).
    
    Example:
        transformer = DeduplicateTransformer(subset=['id'], keep='first')
    """
    
    def __init__(
        self,
        subset: Optional[List[str]] = None,
        keep: Union[str, bool] = "first",
    ):
        self.subset = subset
        self.keep = keep
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates from data."""
        if data.empty:
            return data
        
        return data.drop_duplicates(subset=self.subset, keep=self.keep)


class SortTransformer(Transformer):
    """
    Sort rows by columns.
    
    Args:
        by: Column(s) to sort by.
        ascending: Sort order (True for ascending).
    
    Example:
        transformer = SortTransformer(by=['date', 'id'], ascending=[False, True])
    """
    
    def __init__(
        self,
        by: Union[str, List[str]],
        ascending: Union[bool, List[bool]] = True,
    ):
        if isinstance(by, str):
            by = [by]
        self.by = by
        self.ascending = ascending
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Sort data."""
        if data.empty:
            return data
        
        return data.sort_values(by=self.by, ascending=self.ascending)


class LambdaTransformer(Transformer):
    """
    Apply a custom function to the DataFrame.
    
    Args:
        func: Function that takes and returns a DataFrame.
    
    Example:
        transformer = LambdaTransformer(
            lambda df: df.assign(new_col=df['a'] + df['b'])
        )
    """
    
    def __init__(self, func: Callable[[pd.DataFrame], pd.DataFrame]):
        if not callable(func):
            raise ValueError("func must be callable")
        self.func = func
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply custom function."""
        if data.empty:
            return data
        
        return self.func(data)
