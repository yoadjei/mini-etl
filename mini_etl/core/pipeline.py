from typing import List
from .base import DataSource, Transformer, DataSink
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class Pipeline:
    """
    Boss class that runs the ETL show.
    """
    def __init__(self):
        self._source = None
        self._transformers = []
        self._sink = None

    def set_source(self, source: DataSource):
        self._source = source
        return self

    def add_transformer(self, transformer: Transformer):
        self._transformers.append(transformer)
        return self

    def set_sink(self, sink: DataSink):
        self._sink = sink
        return self

    def run(self):
        if not self._source or not self._sink:
            raise ValueError("Need a source and a sink to run.")

        logger.info("kicking off pipeline...")
        start_time = time.time()
        
        # Pull data
        stream = self._source.extract()

        # Apply transforms lazily
        for t in self._transformers:
            stream = self._apply_transformer(stream, t)

        # Count rows while flowing
        stream = self._monitor_stream(stream)

        # Write to destination
        self._sink.load(stream)
        
        duration = time.time() - start_time
        logger.info(f"Done. Processed {self._row_count} rows in {duration:.4f}s.")

    def _monitor_stream(self, stream):
        self._row_count = 0
        for chunk in stream:
            self._row_count += len(chunk)
            yield chunk

    def _apply_transformer(self, stream, transformer):
        for chunk in stream:
            yield transformer.transform(chunk)
