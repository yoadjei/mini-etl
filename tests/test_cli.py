"""
Tests for CLI module.
"""

import pytest
from click.testing import CliRunner
from pathlib import Path

from mini_etl.cli import main


@pytest.fixture
def runner():
    """Create CLI test runner."""
    return CliRunner()


class TestCLI:
    """Tests for CLI commands."""
    
    def test_help(self, runner):
        """Test --help flag."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "Mini-ETL" in result.output
    
    def test_version(self, runner):
        """Test --version flag."""
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output
    
    def test_init_command(self, runner, temp_dir: Path):
        """Test init command."""
        with runner.isolated_filesystem(temp_dir=str(temp_dir)):
            result = runner.invoke(main, ["init", "-o", "test.yaml"])
            assert result.exit_code == 0
            assert Path("test.yaml").exists()
    
    def test_validate_command(self, runner, sample_config_file: Path):
        """Test validate command."""
        result = runner.invoke(main, ["validate", str(sample_config_file)])
        assert result.exit_code == 0
        assert "PASSED" in result.output
    
    def test_info_command(self, runner):
        """Test info command."""
        result = runner.invoke(main, ["info"])
        assert result.exit_code == 0
        assert "mini-etl" in result.output
