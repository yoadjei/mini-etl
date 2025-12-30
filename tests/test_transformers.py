"""
Tests for transformers module.
"""

import pytest
import pandas as pd
import numpy as np

from mini_etl.components.transformers import (
    FilterTransformer,
    RenameTransformer,
    SelectColumnsTransformer,
    DropColumnsTransformer,
    CastTypeTransformer,
    FillNATransformer,
    ExpressionTransformer,
    GroupAggTransformer,
    StatefulAggTransformer,
    DeduplicateTransformer,
    SortTransformer,
    LambdaTransformer,
)


class TestFilterTransformer:
    """Tests for FilterTransformer."""
    
    def test_filter_basic(self, small_df: pd.DataFrame):
        """Test basic filtering."""
        transformer = FilterTransformer(lambda df: df["value"] > 15)
        result = transformer.transform(small_df)
        
        assert len(result) == 2
        assert result["value"].min() > 15
    
    def test_filter_all(self, small_df: pd.DataFrame):
        """Test filter that matches all rows."""
        transformer = FilterTransformer(lambda df: df["value"] > 0)
        result = transformer.transform(small_df)
        
        assert len(result) == len(small_df)
    
    def test_filter_none(self, small_df: pd.DataFrame):
        """Test filter that matches no rows."""
        transformer = FilterTransformer(lambda df: df["value"] > 100)
        result = transformer.transform(small_df)
        
        assert len(result) == 0
    
    def test_filter_empty_df(self, empty_df: pd.DataFrame):
        """Test filter on empty DataFrame."""
        transformer = FilterTransformer(lambda df: df["value"] > 0)
        result = transformer.transform(empty_df)
        
        assert len(result) == 0
    
    def test_filter_multiple_conditions(self, sample_df: pd.DataFrame):
        """Test filter with multiple conditions."""
        transformer = FilterTransformer(
            lambda df: (df["value"] > 150) & (df["category"] == "A")
        )
        result = transformer.transform(sample_df)
        
        assert all(result["value"] > 150)
        assert all(result["category"] == "A")
    
    def test_invalid_condition(self):
        """Test validation of condition."""
        with pytest.raises(ValueError):
            FilterTransformer("not a callable")


class TestRenameTransformer:
    """Tests for RenameTransformer."""
    
    def test_rename_basic(self, small_df: pd.DataFrame):
        """Test basic renaming."""
        transformer = RenameTransformer({"value": "amount"})
        result = transformer.transform(small_df)
        
        assert "amount" in result.columns
        assert "value" not in result.columns
    
    def test_rename_multiple(self, small_df: pd.DataFrame):
        """Test renaming multiple columns."""
        transformer = RenameTransformer({"value": "amount", "name": "title"})
        result = transformer.transform(small_df)
        
        assert "amount" in result.columns
        assert "title" in result.columns
    
    def test_rename_nonexistent_column(self, small_df: pd.DataFrame):
        """Test renaming column that doesn't exist (should warn, not fail)."""
        transformer = RenameTransformer({"nonexistent": "new_name"})
        result = transformer.transform(small_df)
        
        # Should not modify DataFrame
        assert list(result.columns) == list(small_df.columns)
    
    def test_rename_empty_df(self, empty_df: pd.DataFrame):
        """Test rename on empty DataFrame."""
        transformer = RenameTransformer({"value": "amount"})
        result = transformer.transform(empty_df)
        
        assert len(result) == 0


class TestSelectColumnsTransformer:
    """Tests for SelectColumnsTransformer."""
    
    def test_select_basic(self, small_df: pd.DataFrame):
        """Test basic column selection."""
        transformer = SelectColumnsTransformer(["id", "value"])
        result = transformer.transform(small_df)
        
        assert list(result.columns) == ["id", "value"]
    
    def test_select_single(self, small_df: pd.DataFrame):
        """Test selecting single column."""
        transformer = SelectColumnsTransformer(["id"])
        result = transformer.transform(small_df)
        
        assert list(result.columns) == ["id"]
    
    def test_select_nonexistent_ignore(self, small_df: pd.DataFrame):
        """Test selecting nonexistent column with ignore_missing=True."""
        transformer = SelectColumnsTransformer(
            ["id", "nonexistent"],
            ignore_missing=True,
        )
        result = transformer.transform(small_df)
        
        assert list(result.columns) == ["id"]
    
    def test_select_nonexistent_error(self, small_df: pd.DataFrame):
        """Test selecting nonexistent column with ignore_missing=False."""
        transformer = SelectColumnsTransformer(
            ["id", "nonexistent"],
            ignore_missing=False,
        )
        
        with pytest.raises(ValueError):
            transformer.transform(small_df)


class TestDropColumnsTransformer:
    """Tests for DropColumnsTransformer."""
    
    def test_drop_basic(self, small_df: pd.DataFrame):
        """Test basic column dropping."""
        transformer = DropColumnsTransformer(["name"])
        result = transformer.transform(small_df)
        
        assert "name" not in result.columns
        assert "id" in result.columns
        assert "value" in result.columns
    
    def test_drop_multiple(self, small_df: pd.DataFrame):
        """Test dropping multiple columns."""
        transformer = DropColumnsTransformer(["name", "value"])
        result = transformer.transform(small_df)
        
        assert list(result.columns) == ["id"]


class TestCastTypeTransformer:
    """Tests for CastTypeTransformer."""
    
    def test_cast_to_float(self, small_df: pd.DataFrame):
        """Test casting to float."""
        transformer = CastTypeTransformer({"id": "float"})
        result = transformer.transform(small_df)
        
        assert result["id"].dtype == "float64"
    
    def test_cast_to_string(self, small_df: pd.DataFrame):
        """Test casting to string."""
        transformer = CastTypeTransformer({"id": "str"})
        result = transformer.transform(small_df)
        
        assert result["id"].dtype == "object"
    
    def test_cast_multiple(self, small_df: pd.DataFrame):
        """Test casting multiple columns."""
        transformer = CastTypeTransformer({"id": "float", "value": "str"})
        result = transformer.transform(small_df)
        
        assert result["id"].dtype == "float64"
        assert result["value"].dtype == "object"


class TestFillNATransformer:
    """Tests for FillNATransformer."""
    
    def test_fill_scalar(self, df_with_nulls: pd.DataFrame):
        """Test filling with scalar value."""
        transformer = FillNATransformer(value=0)
        result = transformer.transform(df_with_nulls)
        
        # Numeric columns should be filled
        assert result["value"].isna().sum() == 0
    
    def test_fill_dict(self, df_with_nulls: pd.DataFrame):
        """Test filling with different values per column."""
        transformer = FillNATransformer(value={"value": 0, "name": "Unknown"})
        result = transformer.transform(df_with_nulls)
        
        assert result["value"].isna().sum() == 0
        assert result["name"].isna().sum() == 0
    
    def test_fill_specific_columns(self, df_with_nulls: pd.DataFrame):
        """Test filling only specific columns."""
        transformer = FillNATransformer(value=0, columns=["value"])
        result = transformer.transform(df_with_nulls)
        
        assert result["value"].isna().sum() == 0
        # Other columns should still have nulls
        assert result["name"].isna().sum() > 0


class TestExpressionTransformer:
    """Tests for ExpressionTransformer."""
    
    def test_create_column(self, small_df: pd.DataFrame):
        """Test creating new column with expression."""
        transformer = ExpressionTransformer("double_value = value * 2")
        result = transformer.transform(small_df)
        
        assert "double_value" in result.columns
        assert result["double_value"].iloc[0] == 20  # 10 * 2
    
    def test_filter_mode(self, small_df: pd.DataFrame):
        """Test using expression as filter."""
        transformer = ExpressionTransformer("value > 15", filter_mode=True)
        result = transformer.transform(small_df)
        
        assert len(result) == 2
        assert all(result["value"] > 15)


class TestGroupAggTransformer:
    """Tests for GroupAggTransformer."""
    
    def test_sum_aggregation(self, sample_df: pd.DataFrame):
        """Test sum aggregation."""
        transformer = GroupAggTransformer(
            group_by=["category"],
            agg={"value": "sum"},
        )
        result = transformer.transform(sample_df)
        
        assert len(result) == 5  # 5 categories
        assert "category" in result.columns
        assert "value" in result.columns
    
    def test_multiple_aggregations(self, sample_df: pd.DataFrame):
        """Test multiple aggregation functions."""
        transformer = GroupAggTransformer(
            group_by=["category"],
            agg={"value": ["sum", "mean", "count"]},
        )
        result = transformer.transform(sample_df)
        
        # Should have flattened column names
        assert len(result.columns) > 2


class TestStatefulAggTransformer:
    """Tests for StatefulAggTransformer."""
    
    def test_stateful_sum(self, sample_df: pd.DataFrame):
        """Test stateful sum aggregation across chunks."""
        transformer = StatefulAggTransformer(
            group_by=["category"],
            agg={"value": "sum"},
        )
        
        # Process in chunks
        chunk1 = sample_df.head(50)
        chunk2 = sample_df.tail(50)
        
        transformer.transform(chunk1)
        transformer.transform(chunk2)
        
        result = transformer.finalize()
        
        assert len(result) == 5
        # Verify sums are correct
        expected = sample_df.groupby("category")["value"].sum()
        for _, row in result.iterrows():
            assert row["value"] == expected[row["category"]]
    
    def test_reset(self, sample_df: pd.DataFrame):
        """Test resetting stateful transformer."""
        transformer = StatefulAggTransformer(
            group_by=["category"],
            agg={"value": "sum"},
        )
        
        transformer.transform(sample_df)
        transformer.reset()
        
        result = transformer.finalize()
        assert len(result) == 0


class TestDeduplicateTransformer:
    """Tests for DeduplicateTransformer."""
    
    def test_dedupe_all_columns(self):
        """Test deduplication on all columns."""
        df = pd.DataFrame({
            "id": [1, 1, 2, 2, 3],
            "value": [10, 10, 20, 20, 30],
        })
        
        transformer = DeduplicateTransformer()
        result = transformer.transform(df)
        
        assert len(result) == 3
    
    def test_dedupe_subset(self):
        """Test deduplication on subset of columns."""
        df = pd.DataFrame({
            "id": [1, 1, 2, 2, 3],
            "value": [10, 11, 20, 21, 30],
        })
        
        transformer = DeduplicateTransformer(subset=["id"])
        result = transformer.transform(df)
        
        assert len(result) == 3


class TestSortTransformer:
    """Tests for SortTransformer."""
    
    def test_sort_ascending(self, small_df: pd.DataFrame):
        """Test ascending sort."""
        # Shuffle first
        shuffled = small_df.sample(frac=1)
        
        transformer = SortTransformer(by="id", ascending=True)
        result = transformer.transform(shuffled)
        
        assert list(result["id"]) == [1, 2, 3]
    
    def test_sort_descending(self, small_df: pd.DataFrame):
        """Test descending sort."""
        transformer = SortTransformer(by="value", ascending=False)
        result = transformer.transform(small_df)
        
        assert list(result["value"]) == [30, 20, 10]


class TestLambdaTransformer:
    """Tests for LambdaTransformer."""
    
    def test_custom_function(self, small_df: pd.DataFrame):
        """Test applying custom function."""
        transformer = LambdaTransformer(
            lambda df: df.assign(new_col=df["value"] * 2)
        )
        result = transformer.transform(small_df)
        
        assert "new_col" in result.columns
        assert list(result["new_col"]) == [20, 40, 60]
    
    def test_invalid_function(self):
        """Test validation of callable."""
        with pytest.raises(ValueError):
            LambdaTransformer("not a function")
