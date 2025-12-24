# Mini ETL (MVP)

A lightweight, streaming ETL system in Python. No complex schedulers, just code.

## features

*   **streaming**: handles large files via chunking (csv, json)
*   **transformers**: filter, rename, aggregate
*   **flexible sinks**: write to csv, jsonl, sqlite, parquet
*   **monitoring**: auto-logs row counts and execution time

## quick start

1.  install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  run the demo:
    ```bash
    python demo.py
    ```

## usage

```python
from mini_etl.core.pipeline import Pipeline
from mini_etl.components.extractors import CSVExtractor
from mini_etl.components.transformers import FilterTransformer
from mini_etl.components.loaders import SQLLoader

# define pipeline
pipeline = Pipeline()
source = CSVExtractor('data.csv', chunksize=5000)
loader = SQLLoader('sqlite:///data.db', 'users')

pipeline.set_source(source)\
        .add_transformer(FilterTransformer(lambda df: df['active'] == True))\
        .set_sink(loader)

# run
pipeline.run()
```
