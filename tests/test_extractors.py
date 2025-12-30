"""
Tests for extractors module.
"""

import pytest
import pandas as pd
from pathlib import Path

from mini_etl.components.extractors import (
    CSVExtractor,
    JSONExtractor,
    ExcelExtractor,
    ParquetExtractor,
    SQLExtractor,
    InMemoryExtractor,
)


class TestCSVExtractor:
    """Tests for CSVExtractor."""
    
    def test_extract_basic(self, sample_csv: Path):
        """Test basic CSV extraction."""
        extractor = CSVExtractor(str(sample_csv), chunksize=50)
        chunks = list(extractor.extract())
        
        assert len(chunks) == 2  # 100 rows / 50 chunksize
        assert len(chunks[0]) == 50
        assert len(chunks[1]) == 50
    
    def test_extract_all_at_once(self, sample_csv: Path):
        """Test extraction with large chunksize (single chunk)."""
        extractor = CSVExtractor(str(sample_csv), chunksize=1000)
        chunks = list(extractor.extract())
        
        assert len(chunks) == 1
        assert len(chunks[0]) == 100
    
    def test_extract_columns(self, sample_csv: Path):
        """Test extracting specific columns."""
        extractor = CSVExtractor(
            str(sample_csv),
            chunksize=100,
            usecols=["id", "value"],
        )
        chunks = list(extractor.extract())
        
        assert len(chunks[0].columns) == 2
        assert "id" in chunks[0].columns
        assert "value" in chunks[0].columns
    
    def test_file_not_found(self, temp_dir: Path):
        """Test error handling for missing file."""
        extractor = CSVExtractor(str(temp_dir / "nonexistent.csv"))
        
        with pytest.raises(FileNotFoundError):
            list(extractor.extract())
    
    def test_empty_file(self, temp_dir: Path):
        """Test handling empty CSV file."""
        empty_file = temp_dir / "empty.csv"
        empty_file.write_text("")
        
        extractor = CSVExtractor(str(empty_file))
        chunks = list(extractor.extract())
        
        assert len(chunks) == 0
    
    def test_missing_filepath(self):
        """Test validation of required filepath."""
        with pytest.raises(ValueError):
            CSVExtractor("")


class TestJSONExtractor:
    """Tests for JSONExtractor."""
    
    def test_extract_jsonl(self, sample_jsonl: Path):
        """Test JSONL extraction."""
        extractor = JSONExtractor(str(sample_jsonl), lines=True, chunksize=50)
        chunks = list(extractor.extract())
        
        assert len(chunks) == 2
        assert len(chunks[0]) == 50
    
    def test_extract_json_array(self, sample_json: Path):
        """Test JSON array extraction."""
        extractor = JSONExtractor(str(sample_json), lines=False)
        chunks = list(extractor.extract())
        
        assert len(chunks) == 1
        assert len(chunks[0]) == 100
    
    def test_file_not_found(self, temp_dir: Path):
        """Test error handling for missing file."""
        extractor = JSONExtractor(str(temp_dir / "nonexistent.json"))
        
        with pytest.raises(FileNotFoundError):
            list(extractor.extract())


class TestParquetExtractor:
    """Tests for ParquetExtractor."""
    
    def test_extract_basic(self, sample_parquet: Path):
        """Test basic Parquet extraction."""
        extractor = ParquetExtractor(str(sample_parquet), chunksize=50)
        chunks = list(extractor.extract())
        
        assert len(chunks) == 2
        assert len(chunks[0]) == 50
    
    def test_column_selection(self, sample_parquet: Path):
        """Test extracting specific columns."""
        extractor = ParquetExtractor(
            str(sample_parquet),
            columns=["id", "value"],
        )
        chunks = list(extractor.extract())
        
        assert len(chunks[0].columns) == 2
    
    def test_file_not_found(self, temp_dir: Path):
        """Test error handling for missing file."""
        extractor = ParquetExtractor(str(temp_dir / "nonexistent.parquet"))
        
        with pytest.raises(FileNotFoundError):
            list(extractor.extract())


class TestExcelExtractor:
    """Tests for ExcelExtractor."""
    
    def test_extract_basic(self, sample_excel: Path):
        """Test basic Excel extraction."""
        extractor = ExcelExtractor(str(sample_excel), chunksize=50)
        chunks = list(extractor.extract())
        
        assert len(chunks) == 2
        assert len(chunks[0]) == 50
    
    def test_file_not_found(self, temp_dir: Path):
        """Test error handling for missing file."""
        extractor = ExcelExtractor(str(temp_dir / "nonexistent.xlsx"))
        
        with pytest.raises(FileNotFoundError):
            list(extractor.extract())


class TestSQLExtractor:
    """Tests for SQLExtractor."""
    
    def test_extract_table(self, sample_sqlite: str):
        """Test extraction from SQL table."""
        extractor = SQLExtractor(sample_sqlite, table="test_table", chunksize=50)
        chunks = list(extractor.extract())
        
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == 100
    
    def test_extract_query(self, sample_sqlite: str):
        """Test extraction using SQL query."""
        extractor = SQLExtractor(
            sample_sqlite,
            query="SELECT * FROM test_table WHERE value > 150",
            chunksize=100,
        )
        chunks = list(extractor.extract())
        
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == 49  # values 151-199
    
    def test_missing_connection_string(self):
        """Test validation of required connection string."""
        with pytest.raises(ValueError):
            SQLExtractor("", table="test")
    
    def test_missing_table_and_query(self, sample_sqlite: str):
        """Test validation requiring table or query."""
        with pytest.raises(ValueError):
            SQLExtractor(sample_sqlite)
    
    def test_both_table_and_query(self, sample_sqlite: str):
        """Test validation preventing both table and query."""
        with pytest.raises(ValueError):
            SQLExtractor(sample_sqlite, table="test", query="SELECT 1")


class TestInMemoryExtractor:
    """Tests for InMemoryExtractor."""
    
    def test_single_dataframe(self, sample_df: pd.DataFrame):
        """Test extraction from single DataFrame."""
        extractor = InMemoryExtractor(sample_df)
        chunks = list(extractor.extract())
        
        assert len(chunks) == 1
        assert len(chunks[0]) == 100
    
    def test_dataframe_list(self, sample_df: pd.DataFrame):
        """Test extraction from list of DataFrames."""
        df1 = sample_df.head(50)
        df2 = sample_df.tail(50)
        
        extractor = InMemoryExtractor([df1, df2])
        chunks = list(extractor.extract())
        
        assert len(chunks) == 2
    
    def test_with_chunking(self, sample_df: pd.DataFrame):
        """Test extraction with custom chunksize."""
        extractor = InMemoryExtractor(sample_df, chunksize=25)
        chunks = list(extractor.extract())
        
        assert len(chunks) == 4
        assert all(len(c) == 25 for c in chunks)
