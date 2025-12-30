"""
Mini-ETL: A lightweight, streaming ETL system in Python.

Simple, code-first ETL pipelines with streaming support for large datasets.
"""

__version__ = "0.1.0"
__author__ = "Adjei"

from mini_etl.core.pipeline import Pipeline
from mini_etl.core.base import DataSource, Transformer, DataSink
from mini_etl.components.extractors import (
    CSVExtractor,
    JSONExtractor,
    ExcelExtractor,
    ParquetExtractor,
    SQLExtractor,
    APIExtractor,
)
from mini_etl.components.transformers import (
    FilterTransformer,
    RenameTransformer,
    GroupAggTransformer,
    SelectColumnsTransformer,
    DropColumnsTransformer,
    CastTypeTransformer,
    FillNATransformer,
    ExpressionTransformer,
)
from mini_etl.components.loaders import (
    CSVLoader,
    JSONLoader,
    SQLLoader,
    ParquetLoader,
    ExcelLoader,
)

__all__ = [
    # Core
    "Pipeline",
    "DataSource",
    "Transformer",
    "DataSink",
    # Extractors
    "CSVExtractor",
    "JSONExtractor",
    "ExcelExtractor",
    "ParquetExtractor",
    "SQLExtractor",
    "APIExtractor",
    # Transformers
    "FilterTransformer",
    "RenameTransformer",
    "GroupAggTransformer",
    "SelectColumnsTransformer",
    "DropColumnsTransformer",
    "CastTypeTransformer",
    "FillNATransformer",
    "ExpressionTransformer",
    # Loaders
    "CSVLoader",
    "JSONLoader",
    "SQLLoader",
    "ParquetLoader",
    "ExcelLoader",
]
