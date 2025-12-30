"""
Tests for pipeline module.
"""

import pytest
import pandas as pd
from pathlib import Path

from mini_etl.core.pipeline import Pipeline, PipelineBuilder
from mini_etl.components.extractors import CSVExtractor, InMemoryExtractor
from mini_etl.components.transformers import FilterTransformer, RenameTransformer
from mini_etl.components.loaders import CSVLoader, NullLoader


class TestPipeline:
    """Tests for Pipeline class."""
    
    def test_basic_pipeline(self, sample_csv: Path, temp_dir: Path):
        """Test basic pipeline execution."""
        output_path = temp_dir / "output.csv"
        
        pipeline = Pipeline(name="test", show_progress=False)
        pipeline.set_source(CSVExtractor(str(sample_csv), chunksize=25))
        pipeline.add_transformer(FilterTransformer(lambda df: df["value"] > 150))
        pipeline.set_sink(CSVLoader(str(output_path)))
        
        stats = pipeline.run()
        
        assert stats["rows"] == 49  # values 151-199
        assert stats["chunks"] > 0
        assert output_path.exists()
    
    def test_pipeline_chaining(self, sample_csv: Path, temp_dir: Path):
        """Test fluent interface chaining."""
        output_path = temp_dir / "output.csv"
        
        pipeline = (
            Pipeline(name="chained", show_progress=False)
            .set_source(CSVExtractor(str(sample_csv), chunksize=50))
            .add_transformer(FilterTransformer(lambda df: df["value"] > 150))
            .add_transformer(RenameTransformer({"value": "amount"}))
            .set_sink(CSVLoader(str(output_path)))
        )
        
        stats = pipeline.run()
        
        result = pd.read_csv(output_path)
        assert "amount" in result.columns
        assert "value" not in result.columns
    
    def test_pipeline_no_source(self, temp_dir: Path):
        """Test error when source is missing."""
        pipeline = Pipeline(show_progress=False)
        pipeline.set_sink(CSVLoader(str(temp_dir / "output.csv")))
        
        with pytest.raises(ValueError, match="source"):
            pipeline.run()
    
    def test_pipeline_no_sink(self, sample_csv: Path):
        """Test error when sink is missing."""
        pipeline = Pipeline(show_progress=False)
        pipeline.set_source(CSVExtractor(str(sample_csv)))
        
        with pytest.raises(ValueError, match="sink"):
            pipeline.run()
    
    def test_pipeline_empty_data(self, temp_dir: Path):
        """Test pipeline with no matching data."""
        # Create empty CSV
        empty_csv = temp_dir / "empty.csv"
        pd.DataFrame(columns=["id", "value"]).to_csv(empty_csv, index=False)
        
        output_path = temp_dir / "output.csv"
        
        pipeline = Pipeline(show_progress=False)
        pipeline.set_source(CSVExtractor(str(empty_csv)))
        pipeline.set_sink(CSVLoader(str(output_path)))
        
        stats = pipeline.run()
        
        assert stats["rows"] == 0
    
    def test_pipeline_callbacks(self, sample_df: pd.DataFrame):
        """Test pipeline callbacks."""
        chunk_sizes = []
        
        def on_chunk_complete(chunk_num, size):
            chunk_sizes.append(size)
        
        pipeline = Pipeline(show_progress=False)
        pipeline.set_source(InMemoryExtractor(sample_df, chunksize=25))
        pipeline.set_sink(NullLoader(log_stats=False))
        pipeline.on_chunk_complete(on_chunk_complete)
        
        pipeline.run()
        
        assert len(chunk_sizes) == 4
        assert sum(chunk_sizes) == 100
    
    def test_pipeline_error_handling_raise(self, sample_df: pd.DataFrame):
        """Test error handling in raise mode."""
        def bad_filter(df):
            raise ValueError("Intentional error")
        
        pipeline = Pipeline(show_progress=False, on_error="raise")
        pipeline.set_source(InMemoryExtractor(sample_df, chunksize=25))
        pipeline.add_transformer(FilterTransformer(bad_filter))
        pipeline.set_sink(NullLoader(log_stats=False))
        
        with pytest.raises(ValueError, match="Intentional"):
            pipeline.run()
    
    def test_pipeline_error_handling_skip(self, sample_df: pd.DataFrame):
        """Test error handling in skip mode."""
        call_count = [0]
        
        def sometimes_bad_filter(df):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("Intentional error")
            return df["value"] > 0
        
        pipeline = Pipeline(show_progress=False, on_error="skip")
        pipeline.set_source(InMemoryExtractor(sample_df, chunksize=25))
        pipeline.add_transformer(FilterTransformer(sometimes_bad_filter))
        pipeline.set_sink(NullLoader(log_stats=False))
        
        stats = pipeline.run()
        
        # Should process 3 out of 4 chunks (one skipped)
        assert stats["errors"] == 1
        assert stats["rows"] == 75
    
    def test_pipeline_copy(self, sample_df: pd.DataFrame):
        """Test pipeline copy."""
        pipeline = Pipeline(name="original", show_progress=False)
        pipeline.set_source(InMemoryExtractor(sample_df))
        pipeline.add_transformer(FilterTransformer(lambda df: df["value"] > 0))
        pipeline.set_sink(NullLoader(log_stats=False))
        
        copied = pipeline.copy()
        
        assert copied.name == "original"
        assert len(copied._transformers) == 1
    
    def test_pipeline_clear(self, sample_df: pd.DataFrame):
        """Test pipeline clear."""
        pipeline = Pipeline(show_progress=False)
        pipeline.set_source(InMemoryExtractor(sample_df))
        pipeline.set_sink(NullLoader(log_stats=False))
        
        pipeline.clear()
        
        assert pipeline._source is None
        assert pipeline._sink is None


class TestPipelineBuilder:
    """Tests for PipelineBuilder class."""
    
    def test_builder_csv_workflow(self, sample_csv: Path, temp_dir: Path):
        """Test builder for CSV to CSV workflow."""
        output_path = temp_dir / "output.csv"
        
        pipeline = (
            PipelineBuilder("builder_test")
            .from_csv(str(sample_csv), chunksize=50)
            .filter(lambda df: df["value"] > 150)
            .rename({"value": "amount"})
            .with_progress(False)
            .to_csv(str(output_path))
            .build()
        )
        
        stats = pipeline.run()
        
        assert stats["rows"] == 49
        
        result = pd.read_csv(output_path)
        assert "amount" in result.columns
    
    def test_builder_select_drop(self, sample_csv: Path, temp_dir: Path):
        """Test builder select and drop methods."""
        output_path = temp_dir / "output.csv"
        
        pipeline = (
            PipelineBuilder("select_test")
            .from_csv(str(sample_csv))
            .select(["id", "value", "category"])
            .drop(["category"])
            .with_progress(False)
            .to_csv(str(output_path))
            .build()
        )
        
        pipeline.run()
        
        result = pd.read_csv(output_path)
        assert list(result.columns) == ["id", "value"]
    
    def test_builder_custom_transform(self, sample_csv: Path, temp_dir: Path):
        """Test builder with custom transform."""
        output_path = temp_dir / "output.csv"
        
        pipeline = (
            PipelineBuilder("transform_test")
            .from_csv(str(sample_csv))
            .transform(lambda df: df.assign(doubled=df["value"] * 2))
            .with_progress(False)
            .to_csv(str(output_path))
            .build()
        )
        
        pipeline.run()
        
        result = pd.read_csv(output_path)
        assert "doubled" in result.columns
