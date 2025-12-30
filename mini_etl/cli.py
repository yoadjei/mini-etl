"""
Command-line interface for mini-etl.
"""

import sys
import logging
from pathlib import Path
from typing import Optional

import click

from mini_etl import __version__


@click.group()
@click.version_option(version=__version__, prog_name="mini-etl")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def main(verbose: bool):
    """Mini-ETL: A lightweight, streaming ETL system."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


@main.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Validate config without running")
@click.option("--no-progress", is_flag=True, help="Disable progress bar")
def run(config_file: str, dry_run: bool, no_progress: bool):
    """
    Run a pipeline from a configuration file.
    
    CONFIG_FILE: Path to YAML or JSON configuration file.
    
    Example:
    
        mini-etl run pipeline.yaml
        
        mini-etl run config.json --dry-run
    """
    from mini_etl.core.config import ConfigLoader
    
    click.echo(f"Loading configuration: {config_file}")
    
    try:
        loader = ConfigLoader()
        config = loader.load(config_file)
        
        # Validate
        errors = config.validate()
        if errors:
            click.echo(click.style("Configuration errors:", fg="red"))
            for error in errors:
                click.echo(f"  - {error}")
            sys.exit(1)
        
        click.echo(click.style("Configuration valid", fg="green"))
        click.echo(f"  Pipeline: {config.name}")
        click.echo(f"  Source: {config.source.type if config.source else 'None'}")
        click.echo(f"  Transformers: {len(config.transformers)}")
        click.echo(f"  Sink: {config.sink.type if config.sink else 'None'}")
        
        if dry_run:
            click.echo("\nDry run - skipping execution")
            return
        
        # Build and run pipeline
        pipeline = loader.build_pipeline(config)
        pipeline.show_progress = not no_progress
        
        click.echo("\nRunning pipeline...")
        stats = pipeline.run()
        
        click.echo(click.style("\nPipeline completed successfully", fg="green"))
        click.echo(f"  Rows processed: {stats['rows']:,}")
        click.echo(f"  Chunks: {stats['chunks']}")
        click.echo(f"  Duration: {stats['duration']:.2f}s")
        click.echo(f"  Throughput: {stats['rows_per_second']:,.0f} rows/sec")
        
    except FileNotFoundError as e:
        click.echo(click.style(f"File not found: {e}", fg="red"))
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"Error: {e}", fg="red"))
        if logging.getLogger().level == logging.DEBUG:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@main.command()
@click.argument("config_file", type=click.Path(exists=True))
def validate(config_file: str):
    """
    Validate a pipeline configuration file.
    
    CONFIG_FILE: Path to YAML or JSON configuration file.
    
    Example:
    
        mini-etl validate pipeline.yaml
    """
    from mini_etl.core.config import ConfigLoader
    
    click.echo(f"Validating: {config_file}")
    
    try:
        loader = ConfigLoader()
        config = loader.load(config_file)
        
        errors = config.validate()
        
        if errors:
            click.echo(click.style("Validation FAILED", fg="red"))
            for error in errors:
                click.echo(f"  - {error}")
            sys.exit(1)
        else:
            click.echo(click.style("Validation PASSED", fg="green"))
            click.echo(f"\nPipeline: {config.name}")
            if config.description:
                click.echo(f"Description: {config.description}")
            click.echo(f"Source: {config.source.type}")
            click.echo(f"Transformers: {len(config.transformers)}")
            for i, t in enumerate(config.transformers, 1):
                click.echo(f"  {i}. {t.type}")
            click.echo(f"Sink: {config.sink.type}")
            
    except Exception as e:
        click.echo(click.style(f"Error parsing config: {e}", fg="red"))
        sys.exit(1)


@main.command()
@click.option("-o", "--output", type=click.Path(), default="pipeline.yaml",
              help="Output file path")
@click.option("--format", "fmt", type=click.Choice(["yaml", "json"]), default="yaml",
              help="Configuration format")
def init(output: str, fmt: str):
    """
    Generate a sample pipeline configuration file.
    
    Example:
    
        mini-etl init
        
        mini-etl init -o my_pipeline.yaml
        
        mini-etl init --format json -o config.json
    """
    from mini_etl.core.config import generate_sample_config
    
    content = generate_sample_config()
    
    if fmt == "json":
        import yaml
        import json
        data = yaml.safe_load(content)
        content = json.dumps(data, indent=2)
        if not output.endswith(".json"):
            output = output.rsplit(".", 1)[0] + ".json"
    
    output_path = Path(output)
    
    if output_path.exists():
        if not click.confirm(f"File '{output}' exists. Overwrite?"):
            click.echo("Aborted")
            return
    
    output_path.write_text(content, encoding="utf-8")
    click.echo(click.style(f"Created: {output}", fg="green"))
    click.echo("\nEdit the configuration file, then run:")
    click.echo(f"  mini-etl run {output}")


@main.command()
@click.option("-p", "--port", type=int, default=8501, help="Port to run UI on")
@click.option("--host", default="localhost", help="Host to bind to")
def ui(port: int, host: str):
    """
    Launch the monitoring UI.
    
    Requires the 'ui' extra: pip install mini-etl[ui]
    
    Example:
    
        mini-etl ui
        
        mini-etl ui --port 8080
    """
    try:
        import streamlit
    except ImportError:
        click.echo(click.style(
            "Streamlit not installed. Install with: pip install mini-etl[ui]",
            fg="red"
        ))
        sys.exit(1)
    
    import subprocess
    
    ui_app_path = Path(__file__).parent / "ui" / "app.py"
    
    if not ui_app_path.exists():
        click.echo(click.style("UI module not found", fg="red"))
        sys.exit(1)
    
    click.echo(f"Starting UI at http://{host}:{port}")
    
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        str(ui_app_path),
        "--server.port", str(port),
        "--server.address", host,
        "--browser.gatherUsageStats", "false",
    ])


@main.command()
def info():
    """
    Show information about mini-etl installation.
    """
    import pandas as pd
    
    click.echo(f"mini-etl version: {__version__}")
    click.echo(f"Python version: {sys.version}")
    click.echo(f"Pandas version: {pd.__version__}")
    
    # Check optional dependencies
    click.echo("\nOptional dependencies:")
    
    optional = {
        "pyarrow": "Parquet support",
        "openpyxl": "Excel support",
        "streamlit": "UI support",
        "requests": "API support",
        "tqdm": "Progress bars",
        "tenacity": "Retry support",
    }
    
    for pkg, desc in optional.items():
        try:
            mod = __import__(pkg)
            version = getattr(mod, "__version__", "installed")
            status = click.style(f"[OK] {version}", fg="green")
        except ImportError:
            status = click.style("[NOT INSTALLED]", fg="yellow")
        
        click.echo(f"  {pkg}: {status} - {desc}")


if __name__ == "__main__":
    main()
