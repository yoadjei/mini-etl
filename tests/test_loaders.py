"""
Tests for loaders module.
"""

import os
import pytest
import pandas as pd
from pathlib import Path

from mini_etl.components.loaders import (
    CSVLoader,
    JSONLoader,
    SQLLoader,
    ParquetLoader,
    ExcelLoader,
    NullLoader,
    CallbackLoader,
    MultiLoader,
)


def chunks_from_df(df: pd.DataFrame, chunksize: int = 25):
    """Helper to create chunk generator from DataFrame."""
    for i in range(0, len(df), chunksize):
        yield df.iloc[i:i+chunksize].copy()


class TestCSVLoader:
    """Tests for CSVLoader."""
    
    def test_write_basic(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test basic CSV writing."""
        output_path = temp_dir / "output.csv"
        loader = CSVLoader(str(output_path))
        
        loader.load(chunks_from_df(sample_df, 25))
        
        assert output_path.exists()
        result = pd.read_csv(output_path)
        assert len(result) == 100
    
    def test_write_overwrite(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test overwrite mode."""
        output_path = temp_dir / "output.csv"
        
        # Write first
        loader = CSVLoader(str(output_path), mode="w")
        loader.load(chunks_from_df(sample_df.head(50), 25))
        
        # Overwrite
        loader = CSVLoader(str(output_path), mode="w")
        loader.load(chunks_from_df(sample_df.head(30), 15))
        
        result = pd.read_csv(output_path)
        assert len(result) == 30
    
    def test_write_append(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test append mode."""
        output_path = temp_dir / "output.csv"
        
        # Write first
        loader = CSVLoader(str(output_path), mode="w")
        loader.load(chunks_from_df(sample_df.head(50), 25))
        
        # Append
        loader = CSVLoader(str(output_path), mode="a")
        loader.load(chunks_from_df(sample_df.tail(20), 10))
        
        result = pd.read_csv(output_path)
        assert len(result) == 70
    
    def test_empty_data(self, temp_dir: Path):
        """Test with empty data."""
        output_path = temp_dir / "output.csv"
        loader = CSVLoader(str(output_path))
        
        loader.load(iter([]))
        
        # File should not exist or be empty
        assert not output_path.exists() or output_path.stat().st_size == 0
    
    def test_creates_directory(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test that nested directories are created."""
        output_path = temp_dir / "nested" / "dir" / "output.csv"
        loader = CSVLoader(str(output_path))
        
        loader.load(chunks_from_df(sample_df.head(10), 5))
        
        assert output_path.exists()


class TestJSONLoader:
    """Tests for JSONLoader."""
    
    def test_write_jsonl(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test JSONL writing."""
        output_path = temp_dir / "output.jsonl"
        loader = JSONLoader(str(output_path), lines=True)
        
        loader.load(chunks_from_df(sample_df, 25))
        
        assert output_path.exists()
        result = pd.read_json(output_path, lines=True)
        assert len(result) == 100
    
    def test_write_json_array(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test JSON array writing."""
        output_path = temp_dir / "output.json"
        loader = JSONLoader(str(output_path), lines=False)
        
        loader.load(chunks_from_df(sample_df.head(20), 10))
        
        assert output_path.exists()
        result = pd.read_json(output_path)
        assert len(result) == 20


class TestSQLLoader:
    """Tests for SQLLoader."""
    
    def test_write_basic(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test basic SQL writing."""
        db_path = temp_dir / "output.db"
        connection_string = f"sqlite:///{db_path}"
        
        loader = SQLLoader(connection_string, "output_table", if_exists="replace")
        loader.load(chunks_from_df(sample_df, 25))
        
        # Verify
        from sqlalchemy import create_engine
        engine = create_engine(connection_string)
        result = pd.read_sql("SELECT * FROM output_table", engine)
        engine.dispose()
        
        assert len(result) == 100
    
    def test_write_append(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test append mode."""
        db_path = temp_dir / "output.db"
        connection_string = f"sqlite:///{db_path}"
        
        # First write
        loader = SQLLoader(connection_string, "output_table", if_exists="replace")
        loader.load(chunks_from_df(sample_df.head(50), 25))
        
        # Append
        loader = SQLLoader(connection_string, "output_table", if_exists="append")
        loader.load(chunks_from_df(sample_df.tail(30), 15))
        
        # Verify
        from sqlalchemy import create_engine
        engine = create_engine(connection_string)
        result = pd.read_sql("SELECT * FROM output_table", engine)
        engine.dispose()
        
        assert len(result) == 80


class TestParquetLoader:
    """Tests for ParquetLoader."""
    
    def test_write_basic(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test basic Parquet writing."""
        output_path = temp_dir / "output.parquet"
        loader = ParquetLoader(str(output_path))
        
        loader.load(chunks_from_df(sample_df, 25))
        
        assert output_path.exists()
        result = pd.read_parquet(output_path)
        assert len(result) == 100
    
    def test_compression(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test different compression options."""
        for compression in ["snappy", "gzip"]:
            output_path = temp_dir / f"output_{compression}.parquet"
            loader = ParquetLoader(str(output_path), compression=compression)
            
            loader.load(chunks_from_df(sample_df.head(20), 10))
            
            assert output_path.exists()


class TestExcelLoader:
    """Tests for ExcelLoader."""
    
    def test_write_basic(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test basic Excel writing."""
        output_path = temp_dir / "output.xlsx"
        loader = ExcelLoader(str(output_path))
        
        loader.load(chunks_from_df(sample_df.head(50), 25))
        
        assert output_path.exists()
        result = pd.read_excel(output_path)
        assert len(result) == 50
    
    def test_sheet_name(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test custom sheet name."""
        output_path = temp_dir / "output.xlsx"
        loader = ExcelLoader(str(output_path), sheet_name="MyData")
        
        loader.load(chunks_from_df(sample_df.head(20), 10))
        
        result = pd.read_excel(output_path, sheet_name="MyData")
        assert len(result) == 20


class TestNullLoader:
    """Tests for NullLoader."""
    
    def test_discards_data(self, sample_df: pd.DataFrame):
        """Test that NullLoader consumes but discards data."""
        loader = NullLoader(log_stats=False)
        
        # Should not raise
        loader.load(chunks_from_df(sample_df, 25))


class TestCallbackLoader:
    """Tests for CallbackLoader."""
    
    def test_callback_called(self, sample_df: pd.DataFrame):
        """Test that callback is called for each chunk."""
        chunks_received = []
        
        def callback(df):
            chunks_received.append(len(df))
        
        loader = CallbackLoader(callback)
        loader.load(chunks_from_df(sample_df, 25))
        
        assert len(chunks_received) == 4
        assert sum(chunks_received) == 100
    
    def test_on_complete(self, sample_df: pd.DataFrame):
        """Test on_complete callback."""
        completed = {"called": False}
        
        def on_complete():
            completed["called"] = True
        
        loader = CallbackLoader(lambda df: None, on_complete=on_complete)
        loader.load(chunks_from_df(sample_df.head(10), 5))
        
        assert completed["called"]


class TestMultiLoader:
    """Tests for MultiLoader."""
    
    def test_writes_to_multiple(self, temp_dir: Path, sample_df: pd.DataFrame):
        """Test writing to multiple destinations."""
        csv_path = temp_dir / "output.csv"
        json_path = temp_dir / "output.jsonl"
        
        loader = MultiLoader([
            CSVLoader(str(csv_path)),
            JSONLoader(str(json_path), lines=True),
        ])
        
        loader.load(chunks_from_df(sample_df.head(30), 10))
        
        csv_result = pd.read_csv(csv_path)
        json_result = pd.read_json(json_path, lines=True)
        
        assert len(csv_result) == 30
        assert len(json_result) == 30
