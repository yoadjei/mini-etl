import os
import sys

# Ensure mini_etl is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from mini_etl.core.pipeline import Pipeline
from mini_etl.components.extractors import CSVExtractor
from mini_etl.components.transformers import FilterTransformer, RenameTransformer, GroupAggTransformer
from mini_etl.components.loaders import CSVLoader, JSONLoader, SQLLoader

def main():
    # 1. Create Dummy Data
    data = {
        'id': range(1, 101),
        'category': ['A', 'B', 'A', 'C', 'B'] * 20,
        'value': range(101, 201)
    }
    df = pd.DataFrame(data)
    input_file = 'input.csv'
    output_file = 'output.csv'
    db_file = 'output.db'
    
    df.to_csv(input_file, index=False)
    print(f"Created {input_file} with 100 rows.")

    # 2. Define Pipeline
    # Goal: Filter where value > 150, Rename 'value' to 'score', Group by 'category' and sum 'score'
    
    pipeline = Pipeline()
    
    source = CSVExtractor(input_file, chunksize=20) # Small chunksize to test streaming
    
    # Transformers
    # 1. Filter
    t1 = FilterTransformer(lambda d: d['value'] > 150)
    
    # 2. Rename
    t2 = RenameTransformer({'value': 'score'})
    
    # 3. Aggregation (Note: this is chunk-wise aggregation)
    # This will result in multiple rows per category if they span across chunks.
    t3 = GroupAggTransformer(group_by=['category'], agg={'score': 'sum'})
    
    # Sink
    sink = CSVLoader(output_file, mode='w')
    
    pipeline.set_source(source)\
            .add_transformer(t1)\
            .add_transformer(t2)\
            .add_transformer(t3)\
            .set_sink(sink)
            
    # 3. Run Pipeline (CSV)
    print("Running pipeline (CSV)...")
    pipeline.run()
    
    # 4. Verify Output
    print(f"Pipeline finished. Checking {output_file}...")
    result = pd.read_csv(output_file)
    print("Result head:")
    print(result.head(10))
    print(f"Total rows in result: {len(result)}")

    # 5. Test Parquet (New)
    from mini_etl.components.loaders import ParquetLoader
    pq_file = 'output.parquet'
    print("\nRunning pipeline (Parquet)...")
    
    # Re-instantiate source because it's a generator that is now exhausted
    source_pq = CSVExtractor(input_file, chunksize=20)
    sink_pq = ParquetLoader(pq_file)
    
    pipeline.set_source(source_pq).set_sink(sink_pq)
    pipeline.run()
    print(f"Parquet run finished. Output at {pq_file}.partX.parquet")
    
    # Clean up
    # os.remove(input_file)
    # os.remove(output_file)

if __name__ == "__main__":
    main()
