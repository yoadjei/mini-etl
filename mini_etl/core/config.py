"""
YAML/JSON configuration loader for pipeline definitions.
"""

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import logging

import yaml

logger = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    """Configuration for a data source."""
    type: str
    path: Optional[str] = None
    url: Optional[str] = None
    connection_string: Optional[str] = None
    query: Optional[str] = None
    table: Optional[str] = None
    chunksize: int = 10000
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformerConfig:
    """Configuration for a transformer."""
    type: str
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SinkConfig:
    """Configuration for a data sink."""
    type: str
    path: Optional[str] = None
    connection_string: Optional[str] = None
    table: Optional[str] = None
    mode: str = "overwrite"
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    name: str
    description: str = ""
    source: Optional[SourceConfig] = None
    transformers: List[TransformerConfig] = field(default_factory=list)
    sink: Optional[SinkConfig] = None
    
    # Advanced options
    parallel: bool = False
    workers: int = 4
    retry_attempts: int = 3
    schema: Optional[Dict[str, Any]] = None
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        if not self.name:
            errors.append("Pipeline name is required")
        
        if self.source is None:
            errors.append("Pipeline source is required")
        elif self.source.type not in ("csv", "json", "excel", "parquet", "sql", "api"):
            errors.append(f"Unknown source type: {self.source.type}")
        
        if self.sink is None:
            errors.append("Pipeline sink is required")
        elif self.sink.type not in ("csv", "json", "jsonl", "parquet", "sql", "excel"):
            errors.append(f"Unknown sink type: {self.sink.type}")
        
        for i, t in enumerate(self.transformers):
            valid_types = (
                "filter", "rename", "select", "drop", "cast", 
                "fillna", "expression", "aggregate", "group"
            )
            if t.type not in valid_types:
                errors.append(f"Transformer {i}: Unknown type '{t.type}'")
        
        return errors


class ConfigLoader:
    """
    Loads pipeline configuration from YAML or JSON files.
    
    Supports environment variable substitution using ${VAR} or $VAR syntax.
    
    Example:
        loader = ConfigLoader()
        config = loader.load("pipeline.yaml")
        pipeline = loader.build_pipeline(config)
    """
    
    ENV_VAR_PATTERN = re.compile(r'\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)')
    
    def load(self, filepath: Union[str, Path]) -> PipelineConfig:
        """
        Load configuration from a YAML or JSON file.
        
        Args:
            filepath: Path to configuration file.
        
        Returns:
            PipelineConfig instance.
        
        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If file format is invalid.
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"Configuration file not found: {filepath}")
        
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Substitute environment variables
        content = self._substitute_env_vars(content)
        
        # Parse based on extension
        if filepath.suffix.lower() in (".yaml", ".yml"):
            data = yaml.safe_load(content)
        elif filepath.suffix.lower() == ".json":
            import json
            data = json.loads(content)
        else:
            # Try YAML first, then JSON
            try:
                data = yaml.safe_load(content)
            except yaml.YAMLError:
                import json
                data = json.loads(content)
        
        return self._parse_config(data)
    
    def load_string(self, content: str, format: str = "yaml") -> PipelineConfig:
        """Load configuration from a string."""
        content = self._substitute_env_vars(content)
        
        if format.lower() == "yaml":
            data = yaml.safe_load(content)
        else:
            import json
            data = json.loads(content)
        
        return self._parse_config(data)
    
    def _substitute_env_vars(self, content: str) -> str:
        """Replace ${VAR} and $VAR with environment variable values."""
        def replace(match):
            var_name = match.group(1) or match.group(2)
            value = os.environ.get(var_name)
            if value is None:
                logger.warning(f"Environment variable '{var_name}' not set")
                return match.group(0)  # Keep original if not found
            return value
        
        return self.ENV_VAR_PATTERN.sub(replace, content)
    
    def _parse_config(self, data: Dict[str, Any]) -> PipelineConfig:
        """Parse configuration dictionary into PipelineConfig."""
        if not isinstance(data, dict):
            raise ValueError("Configuration must be a dictionary")
        
        # Handle nested 'pipeline' key
        if "pipeline" in data:
            data = data["pipeline"]
        
        # Parse source
        source = None
        if "source" in data:
            src = data["source"]
            source = SourceConfig(
                type=src.get("type", "csv"),
                path=src.get("path"),
                url=src.get("url"),
                connection_string=src.get("connection_string"),
                query=src.get("query"),
                table=src.get("table"),
                chunksize=src.get("chunksize", 10000),
                options={k: v for k, v in src.items() 
                        if k not in ("type", "path", "url", "connection_string", 
                                    "query", "table", "chunksize")},
            )
        
        # Parse transformers
        transformers = []
        for t in data.get("transformers", []):
            if isinstance(t, str):
                transformers.append(TransformerConfig(type=t))
            else:
                t_type = t.pop("type", "filter")
                transformers.append(TransformerConfig(type=t_type, options=t))
        
        # Parse sink
        sink = None
        if "sink" in data:
            snk = data["sink"]
            sink = SinkConfig(
                type=snk.get("type", "csv"),
                path=snk.get("path"),
                connection_string=snk.get("connection_string"),
                table=snk.get("table"),
                mode=snk.get("mode", "overwrite"),
                options={k: v for k, v in snk.items()
                        if k not in ("type", "path", "connection_string", "table", "mode")},
            )
        
        return PipelineConfig(
            name=data.get("name", "unnamed_pipeline"),
            description=data.get("description", ""),
            source=source,
            transformers=transformers,
            sink=sink,
            parallel=data.get("parallel", False),
            workers=data.get("workers", 4),
            retry_attempts=data.get("retry_attempts", 3),
            schema=data.get("schema"),
        )
    
    def build_pipeline(self, config: PipelineConfig) -> "Pipeline":
        """
        Build a Pipeline object from configuration.
        
        Returns:
            Configured Pipeline ready to run.
        """
        from mini_etl.core.pipeline import Pipeline
        from mini_etl.components import extractors, transformers, loaders
        
        pipeline = Pipeline(name=config.name)
        
        # Build source
        if config.source:
            source = self._build_source(config.source)
            pipeline.set_source(source)
        
        # Build transformers
        for t_config in config.transformers:
            transformer = self._build_transformer(t_config)
            pipeline.add_transformer(transformer)
        
        # Build sink
        if config.sink:
            sink = self._build_sink(config.sink)
            pipeline.set_sink(sink)
        
        return pipeline
    
    def _build_source(self, config: SourceConfig):
        """Build extractor from source config."""
        from mini_etl.components import extractors
        
        source_map = {
            "csv": extractors.CSVExtractor,
            "json": extractors.JSONExtractor,
            "excel": extractors.ExcelExtractor,
            "parquet": extractors.ParquetExtractor,
            "sql": extractors.SQLExtractor,
            "api": extractors.APIExtractor,
        }
        
        cls = source_map.get(config.type)
        if cls is None:
            raise ValueError(f"Unknown source type: {config.type}")
        
        # Build kwargs based on source type
        kwargs = dict(config.options)
        
        if config.type in ("csv", "json", "excel", "parquet"):
            if not config.path:
                raise ValueError(f"{config.type} source requires 'path'")
            kwargs["filepath"] = config.path
            kwargs["chunksize"] = config.chunksize
        elif config.type == "sql":
            if not config.connection_string:
                raise ValueError("SQL source requires 'connection_string'")
            kwargs["connection_string"] = config.connection_string
            kwargs["query"] = config.query
            kwargs["table"] = config.table
            kwargs["chunksize"] = config.chunksize
        elif config.type == "api":
            if not config.url:
                raise ValueError("API source requires 'url'")
            kwargs["url"] = config.url
        
        return cls(**kwargs)
    
    def _build_transformer(self, config: TransformerConfig):
        """Build transformer from config."""
        from mini_etl.components import transformers
        
        opts = config.options
        
        if config.type == "filter":
            # Filter requires a condition - support string expressions
            condition = opts.get("condition")
            if isinstance(condition, str):
                # Convert string to lambda using eval (with safety)
                return transformers.ExpressionTransformer(condition, filter_mode=True)
            return transformers.FilterTransformer(condition)
        
        elif config.type == "rename":
            return transformers.RenameTransformer(opts.get("columns", {}))
        
        elif config.type == "select":
            return transformers.SelectColumnsTransformer(opts.get("columns", []))
        
        elif config.type == "drop":
            return transformers.DropColumnsTransformer(opts.get("columns", []))
        
        elif config.type == "cast":
            return transformers.CastTypeTransformer(opts.get("columns", {}))
        
        elif config.type == "fillna":
            return transformers.FillNATransformer(
                opts.get("value"),
                opts.get("columns"),
                opts.get("method"),
            )
        
        elif config.type == "expression":
            return transformers.ExpressionTransformer(opts.get("expr", ""))
        
        elif config.type in ("aggregate", "group"):
            return transformers.GroupAggTransformer(
                opts.get("group_by", []),
                opts.get("agg", {}),
            )
        
        else:
            raise ValueError(f"Unknown transformer type: {config.type}")
    
    def _build_sink(self, config: SinkConfig):
        """Build loader from sink config."""
        from mini_etl.components import loaders
        
        sink_map = {
            "csv": loaders.CSVLoader,
            "json": loaders.JSONLoader,
            "jsonl": loaders.JSONLoader,
            "parquet": loaders.ParquetLoader,
            "sql": loaders.SQLLoader,
            "excel": loaders.ExcelLoader,
        }
        
        cls = sink_map.get(config.type)
        if cls is None:
            raise ValueError(f"Unknown sink type: {config.type}")
        
        kwargs = dict(config.options)
        
        if config.type in ("csv", "json", "jsonl", "parquet", "excel"):
            if not config.path:
                raise ValueError(f"{config.type} sink requires 'path'")
            kwargs["filepath"] = config.path
            if config.type != "parquet":
                kwargs["mode"] = "w" if config.mode == "overwrite" else "a"
            else:
                kwargs["mode"] = config.mode
        elif config.type == "sql":
            if not config.connection_string:
                raise ValueError("SQL sink requires 'connection_string'")
            kwargs["connection_string"] = config.connection_string
            kwargs["table_name"] = config.table
            kwargs["if_exists"] = "replace" if config.mode == "overwrite" else "append"
        
        return cls(**kwargs)


def generate_sample_config() -> str:
    """Generate a sample pipeline configuration file."""
    return """# Mini-ETL Pipeline Configuration
pipeline:
  name: sample_pipeline
  description: Example data processing pipeline

  source:
    type: csv
    path: data/input.csv
    chunksize: 10000

  transformers:
    - type: filter
      condition: "value > 100"
    
    - type: rename
      columns:
        old_column: new_column
    
    - type: select
      columns:
        - id
        - new_column
        - category

  sink:
    type: parquet
    path: output/processed.parquet
    mode: overwrite

  # Optional settings
  parallel: false
  workers: 4
  retry_attempts: 3
"""
