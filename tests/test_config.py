"""
Tests for configuration module.
"""

import os
import pytest
from pathlib import Path

from mini_etl.core.config import (
    ConfigLoader,
    PipelineConfig,
    SourceConfig,
    SinkConfig,
    TransformerConfig,
    generate_sample_config,
)


class TestConfigLoader:
    """Tests for ConfigLoader."""
    
    def test_load_yaml(self, sample_config_file: Path):
        """Test loading YAML config."""
        loader = ConfigLoader()
        config = loader.load(sample_config_file)
        
        assert config.name == "test_pipeline"
        assert config.source is not None
        assert config.source.type == "csv"
    
    def test_load_string(self, sample_config_yaml: str):
        """Test loading config from string."""
        loader = ConfigLoader()
        config = loader.load_string(sample_config_yaml)
        
        assert config.name == "test_pipeline"
    
    def test_file_not_found(self, temp_dir: Path):
        """Test error for missing file."""
        loader = ConfigLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load(temp_dir / "nonexistent.yaml")


class TestPipelineConfig:
    """Tests for PipelineConfig validation."""
    
    def test_valid_config(self):
        """Test validation of valid config."""
        config = PipelineConfig(
            name="valid",
            source=SourceConfig(type="csv", path="input.csv"),
            sink=SinkConfig(type="csv", path="output.csv"),
        )
        
        errors = config.validate()
        assert len(errors) == 0
    
    def test_missing_name(self):
        """Test validation with missing name."""
        config = PipelineConfig(
            name="",
            source=SourceConfig(type="csv", path="input.csv"),
            sink=SinkConfig(type="csv", path="output.csv"),
        )
        
        errors = config.validate()
        assert any("name" in e.lower() for e in errors)


class TestSampleConfig:
    """Tests for sample config generation."""
    
    def test_generate_sample(self):
        """Test sample config generation."""
        sample = generate_sample_config()
        
        assert "pipeline" in sample
        assert "source" in sample
