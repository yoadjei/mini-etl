"""
Schema definition and validation for data pipelines.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
import logging

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ColumnSchema:
    """Schema definition for a single column."""
    
    name: str
    dtype: str  # pandas dtype string: 'int64', 'float64', 'object', 'datetime64[ns]', etc.
    nullable: bool = True
    default: Optional[Any] = None
    
    def __post_init__(self):
        # Normalize dtype names
        dtype_map = {
            "int": "int64",
            "float": "float64",
            "str": "object",
            "string": "object",
            "bool": "bool",
            "boolean": "bool",
            "datetime": "datetime64[ns]",
            "date": "datetime64[ns]",
        }
        self.dtype = dtype_map.get(self.dtype.lower(), self.dtype)


@dataclass
class Schema:
    """
    Schema definition for a DataFrame.
    
    Example:
        schema = Schema([
            ColumnSchema("id", "int64", nullable=False),
            ColumnSchema("name", "object"),
            ColumnSchema("value", "float64", default=0.0),
        ])
    """
    
    columns: List[ColumnSchema] = field(default_factory=list)
    strict: bool = False  # If True, extra columns cause validation failure
    
    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> "Schema":
        """
        Create Schema from dictionary.
        
        Example:
            schema = Schema.from_dict({
                "columns": {
                    "id": {"dtype": "int64", "nullable": False},
                    "name": {"dtype": "object"},
                },
                "strict": True
            })
        """
        columns = []
        for name, col_def in schema_dict.get("columns", {}).items():
            if isinstance(col_def, str):
                columns.append(ColumnSchema(name=name, dtype=col_def))
            else:
                columns.append(ColumnSchema(
                    name=name,
                    dtype=col_def.get("dtype", "object"),
                    nullable=col_def.get("nullable", True),
                    default=col_def.get("default"),
                ))
        return cls(columns=columns, strict=schema_dict.get("strict", False))
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> "Schema":
        """Infer schema from an existing DataFrame."""
        columns = [
            ColumnSchema(
                name=col,
                dtype=str(df[col].dtype),
                nullable=df[col].isna().any(),
            )
            for col in df.columns
        ]
        return cls(columns=columns)
    
    def get_column(self, name: str) -> Optional[ColumnSchema]:
        """Get column schema by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    @property
    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]


@dataclass
class ValidationResult:
    """Result of schema validation."""
    
    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def __bool__(self) -> bool:
        return self.valid
    
    def raise_if_invalid(self) -> None:
        """Raise ValueError if validation failed."""
        if not self.valid:
            raise ValueError(f"Schema validation failed: {'; '.join(self.errors)}")


class SchemaValidator:
    """
    Validates DataFrames against a schema.
    
    Example:
        schema = Schema([
            ColumnSchema("id", "int64", nullable=False),
            ColumnSchema("value", "float64"),
        ])
        
        validator = SchemaValidator(schema, coerce=True)
        df = validator.validate(df)  # Returns validated/coerced DataFrame
    """
    
    def __init__(
        self,
        schema: Schema,
        coerce: bool = False,
        on_error: str = "raise",  # 'raise', 'warn', 'ignore'
    ):
        self.schema = schema
        self.coerce = coerce
        self.on_error = on_error
        
        if on_error not in ("raise", "warn", "ignore"):
            raise ValueError(f"on_error must be 'raise', 'warn', or 'ignore', got '{on_error}'")
    
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate DataFrame against schema.
        
        Returns:
            Validated (and optionally coerced) DataFrame.
        
        Raises:
            ValueError: If validation fails and on_error='raise'.
        """
        result = self._validate(df)
        
        if not result.valid:
            if self.on_error == "raise":
                result.raise_if_invalid()
            elif self.on_error == "warn":
                for error in result.errors:
                    logger.warning(f"Schema validation: {error}")
        
        for warning in result.warnings:
            logger.warning(f"Schema validation: {warning}")
        
        if self.coerce:
            df = self._coerce(df)
        
        return df
    
    def _validate(self, df: pd.DataFrame) -> ValidationResult:
        """Perform validation and return result."""
        errors: List[str] = []
        warnings: List[str] = []
        
        # Check for missing required columns
        for col_schema in self.schema.columns:
            if col_schema.name not in df.columns:
                if col_schema.default is not None or col_schema.nullable:
                    warnings.append(f"Missing column '{col_schema.name}' (has default/nullable)")
                else:
                    errors.append(f"Missing required column '{col_schema.name}'")
                continue
            
            # Check nullability
            if not col_schema.nullable and df[col_schema.name].isna().any():
                null_count = df[col_schema.name].isna().sum()
                errors.append(
                    f"Column '{col_schema.name}' has {null_count} null values but is not nullable"
                )
            
            # Check dtype (skip if coercion is enabled)
            if not self.coerce:
                actual_dtype = str(df[col_schema.name].dtype)
                if not self._dtypes_compatible(actual_dtype, col_schema.dtype):
                    warnings.append(
                        f"Column '{col_schema.name}' has dtype '{actual_dtype}', "
                        f"expected '{col_schema.dtype}'"
                    )
        
        # Check for extra columns in strict mode
        if self.schema.strict:
            expected_cols = set(self.schema.column_names)
            actual_cols = set(df.columns)
            extra_cols = actual_cols - expected_cols
            if extra_cols:
                errors.append(f"Unexpected columns in strict mode: {extra_cols}")
        
        return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)
    
    def _coerce(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce DataFrame columns to match schema types."""
        df = df.copy()
        
        for col_schema in self.schema.columns:
            if col_schema.name not in df.columns:
                # Add missing column with default
                if col_schema.default is not None:
                    df[col_schema.name] = col_schema.default
                elif col_schema.nullable:
                    df[col_schema.name] = None
                continue
            
            # Coerce type
            try:
                if col_schema.dtype.startswith("datetime"):
                    df[col_schema.name] = pd.to_datetime(df[col_schema.name], errors="coerce")
                elif col_schema.dtype in ("int64", "int32", "int16", "int8"):
                    # Handle nullable integers
                    df[col_schema.name] = pd.to_numeric(df[col_schema.name], errors="coerce")
                    if col_schema.nullable:
                        df[col_schema.name] = df[col_schema.name].astype("Int64")
                    else:
                        df[col_schema.name] = df[col_schema.name].astype(col_schema.dtype)
                elif col_schema.dtype in ("float64", "float32"):
                    df[col_schema.name] = pd.to_numeric(df[col_schema.name], errors="coerce")
                elif col_schema.dtype == "bool":
                    df[col_schema.name] = df[col_schema.name].astype(bool)
                elif col_schema.dtype == "object":
                    df[col_schema.name] = df[col_schema.name].astype(str)
            except Exception as e:
                logger.warning(f"Failed to coerce column '{col_schema.name}': {e}")
        
        return df
    
    def _dtypes_compatible(self, actual: str, expected: str) -> bool:
        """Check if dtypes are compatible (allowing some flexibility)."""
        # Exact match
        if actual == expected:
            return True
        
        # Common compatible groups
        int_types = {"int8", "int16", "int32", "int64", "Int64"}
        float_types = {"float16", "float32", "float64"}
        
        if actual in int_types and expected in int_types:
            return True
        if actual in float_types and expected in float_types:
            return True
        if actual in int_types and expected in float_types:
            return True  # int -> float is fine
        
        return False
