"""
Pytest fixtures and configuration.
"""

import os
import tempfile
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "id": range(1, 101),
        "category": ["A", "B", "C", "D", "E"] * 20,
        "value": range(100, 200),
        "name": [f"item_{i}" for i in range(1, 101)],
    })


@pytest.fixture
def small_df() -> pd.DataFrame:
    """Create a small DataFrame for simple tests."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "value": [10, 20, 30],
        "name": ["a", "b", "c"],
    })


@pytest.fixture
def df_with_nulls() -> pd.DataFrame:
    """Create a DataFrame with null values."""
    return pd.DataFrame({
        "id": [1, 2, None, 4, 5],
        "value": [10.0, None, 30.0, None, 50.0],
        "name": ["a", None, "c", "d", None],
    })


@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Create an empty DataFrame."""
    return pd.DataFrame(columns=["id", "value", "name"])


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for file operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_csv(temp_dir: Path, sample_df: pd.DataFrame) -> Path:
    """Create a sample CSV file."""
    filepath = temp_dir / "sample.csv"
    sample_df.to_csv(filepath, index=False)
    return filepath


@pytest.fixture
def sample_json(temp_dir: Path, sample_df: pd.DataFrame) -> Path:
    """Create a sample JSON file."""
    filepath = temp_dir / "sample.json"
    sample_df.to_json(filepath, orient="records")
    return filepath


@pytest.fixture
def sample_jsonl(temp_dir: Path, sample_df: pd.DataFrame) -> Path:
    """Create a sample JSONL file."""
    filepath = temp_dir / "sample.jsonl"
    sample_df.to_json(filepath, orient="records", lines=True)
    return filepath


@pytest.fixture
def sample_parquet(temp_dir: Path, sample_df: pd.DataFrame) -> Path:
    """Create a sample Parquet file."""
    filepath = temp_dir / "sample.parquet"
    sample_df.to_parquet(filepath, index=False)
    return filepath


@pytest.fixture
def sample_excel(temp_dir: Path, sample_df: pd.DataFrame) -> Path:
    """Create a sample Excel file."""
    filepath = temp_dir / "sample.xlsx"
    sample_df.to_excel(filepath, index=False)
    return filepath


@pytest.fixture
def sample_sqlite(temp_dir: Path, sample_df: pd.DataFrame) -> str:
    """Create a sample SQLite database and return connection string."""
    from sqlalchemy import create_engine
    
    db_path = temp_dir / "sample.db"
    connection_string = f"sqlite:///{db_path}"
    
    engine = create_engine(connection_string)
    sample_df.to_sql("test_table", engine, index=False, if_exists="replace")
    engine.dispose()
    
    return connection_string


@pytest.fixture
def sample_config_yaml() -> str:
    """Return sample YAML configuration."""
    return """
pipeline:
  name: test_pipeline
  description: Test pipeline for unit tests
  
  source:
    type: csv
    path: input.csv
    chunksize: 100
  
  transformers:
    - type: filter
      condition: "value > 50"
    - type: rename
      columns:
        value: amount
  
  sink:
    type: csv
    path: output.csv
    mode: overwrite
"""


@pytest.fixture
def sample_config_file(temp_dir: Path, sample_config_yaml: str) -> Path:
    """Create a sample config file."""
    filepath = temp_dir / "config.yaml"
    filepath.write_text(sample_config_yaml)
    return filepath


# Test data for edge cases
@pytest.fixture
def large_df() -> pd.DataFrame:
    """Create a larger DataFrame for performance tests."""
    import numpy as np
    
    n = 10000
    return pd.DataFrame({
        "id": range(n),
        "category": np.random.choice(["A", "B", "C"], n),
        "value": np.random.randn(n) * 100,
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="h"),
    })


@pytest.fixture
def df_with_special_chars() -> pd.DataFrame:
    """Create DataFrame with special characters in data."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["hello, world", 'quote "test"', "line\nbreak"],
        "description": ["<html>", "a & b", "c > d"],
    })
