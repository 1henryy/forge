from typing import Iterator

import pyarrow.parquet as pq

from forge.datatypes import Schema, Field, RecordBatch, from_arrow_type
from .datasource import DataSource


class ParquetSource(DataSource):
    def __init__(self, path: str) -> None:
        self._path = path

    def schema(self) -> Schema:
        metadata = pq.read_schema(self._path)
        fields = [
            Field(metadata.field(i).name, from_arrow_type(metadata.field(i).type))
            for i in range(len(metadata))
        ]
        return Schema(fields)

    def scan(self, projection: list[int] | None = None) -> Iterator[RecordBatch]:
        parquet_file = pq.ParquetFile(self._path)
        columns: list[str] | None = None
        if projection is not None:
            arrow_schema = parquet_file.schema_arrow
            columns = [arrow_schema.field(i).name for i in projection]
        for arrow_batch in parquet_file.iter_batches(columns=columns):
            yield RecordBatch.from_arrow(arrow_batch)
