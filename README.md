# Mini ETL

A streaming ETL library for Python. Process large files in chunks without loading everything into memory.

## What It Does

- Reads data from CSV, JSON, Excel, Parquet, SQL databases, and REST APIs
- Transforms data with filters, renames, aggregations, type casts, and custom functions
- Writes to CSV, JSON, Parquet, SQL, and Excel
- Streams data in chunks so you can handle files larger than RAM
- Runs from code or from YAML config files
- Includes a monitoring dashboard

## Install

```bash
pip install mini-etl
```

For the dashboard:
```bash
pip install mini-etl[ui]
```

## Basic Usage

```python
from mini_etl import Pipeline, CSVExtractor, FilterTransformer, ParquetLoader

pipeline = Pipeline()
pipeline.set_source(CSVExtractor("input.csv", chunksize=10000))
pipeline.add_transformer(FilterTransformer(lambda df: df["amount"] > 0))
pipeline.set_sink(ParquetLoader("output.parquet"))

stats = pipeline.run()
print(f"Processed {stats['rows']} rows")
```

## Using the Builder

```python
from mini_etl.core.pipeline import PipelineBuilder

pipeline = (
    PipelineBuilder("my_pipeline")
    .from_csv("input.csv")
    .filter(lambda df: df["value"] > 100)
    .rename({"value": "amount"})
    .to_parquet("output.parquet")
    .build()
)

pipeline.run()
```

## Using Config Files

Create `pipeline.yaml`:

```yaml
pipeline:
  name: my_pipeline
  
  source:
    type: csv
    path: input.csv
    chunksize: 10000
  
  transformers:
    - type: filter
      condition: "amount > 0"
    - type: rename
      columns:
        old_name: new_name
  
  sink:
    type: parquet
    path: output.parquet
```

Run it:
```bash
mini-etl run pipeline.yaml
```

## Command Line

```bash
mini-etl run config.yaml      # Run a pipeline
mini-etl validate config.yaml # Check config without running
mini-etl init                 # Generate sample config
mini-etl ui                   # Open dashboard
mini-etl info                 # Show version and dependencies
```

## Available Components

### Sources

| Name | Description |
|------|-------------|
| CSVExtractor | CSV files |
| JSONExtractor | JSON and JSONL files |
| ExcelExtractor | Excel files |
| ParquetExtractor | Parquet files |
| SQLExtractor | SQL databases |
| APIExtractor | REST APIs with pagination |

### Transformers

| Name | Description |
|------|-------------|
| FilterTransformer | Keep rows matching a condition |
| RenameTransformer | Rename columns |
| SelectColumnsTransformer | Keep only specified columns |
| DropColumnsTransformer | Remove columns |
| CastTypeTransformer | Change column types |
| FillNATransformer | Fill missing values |
| GroupAggTransformer | Group and aggregate |
| ExpressionTransformer | Apply pandas expressions |
| DeduplicateTransformer | Remove duplicate rows |
| SortTransformer | Sort rows |
| LambdaTransformer | Apply any function |

### Destinations

| Name | Description |
|------|-------------|
| CSVLoader | CSV files |
| JSONLoader | JSON and JSONL files |
| ParquetLoader | Parquet files |
| SQLLoader | SQL databases |
| ExcelLoader | Excel files |

## Advanced Features

### Parallel Processing

Run transformations across multiple threads or processes:

```python
from mini_etl.core.parallel import ParallelTransformer, ParallelConfig

config = ParallelConfig(workers=4)
parallel = ParallelTransformer(my_transformer, config)
```

### Multi Source Pipelines

Combine data from multiple sources using DAG pipelines:

```python
from mini_etl.core.dag import PipelineDAG, MergeStrategy

dag = PipelineDAG()
dag.add_source("csv", CSVExtractor("data1.csv"))
dag.add_source("api", APIExtractor("https://api.example.com"))
dag.add_merge("combine", strategy=MergeStrategy.CONCAT)
dag.add_sink("output", ParquetLoader("combined.parquet"))

dag.connect("csv", "combine")
dag.connect("api", "combine")
dag.connect("combine", "output")

dag.run()
```

### Scheduling

Run pipelines on a schedule:

```python
from mini_etl.core.scheduler import Scheduler

scheduler = Scheduler()

@scheduler.schedule("0 * * * *")  # Every hour
def hourly_job():
    pipeline.run()

scheduler.start()
```

### Schema Validation

Validate data types before processing:

```python
from mini_etl.core.schema import Schema, ColumnSchema

schema = Schema([
    ColumnSchema("id", "int64", nullable=False),
    ColumnSchema("amount", "float64"),
])

pipeline = Pipeline(validate_schema=True, schema=schema)
```

## Development

```bash
git clone https://github.com/yoadjei/mini-etl.git
cd mini-etl
pip install -e ".[dev]"
pytest tests/
```

## License

MIT
