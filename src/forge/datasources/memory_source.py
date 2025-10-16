from __future__ import annotations

from typing import Iterator

import pyarrow as pa

from forge.datatypes import (
    Schema, Field, RecordBatch, ArrowVector, ColumnVector, from_arrow_type,
)
from .datasource import DataSource


class MemorySource(DataSource):
    def __init__(self, schema: Schema, batches: list[RecordBatch]) -> None:
        self._schema = schema
        self._batches = batches

    def schema(self) -> Schema:
        return self._schema

    def scan(self, projection: list[int] | None = None) -> Iterator[RecordBatch]:
        for batch in self._batches:
            if projection is None:
                yield batch
            else:
                projected_schema = self._schema.project(projection)
                projected_columns: list[ColumnVector] = [
                    batch.column(i) for i in projection
                ]
                yield RecordBatch(projected_schema, projected_columns)

    @classmethod
    def from_pydict(cls, data: dict[str, list]) -> MemorySource:
        arrays = {name: pa.array(values) for name, values in data.items()}
        fields = [
            Field(name, from_arrow_type(arrays[name].type))
            for name in data
        ]
        schema = Schema(fields)
        columns: list[ColumnVector] = [ArrowVector(arrays[name]) for name in data]
        batch = RecordBatch(schema, columns)
        return cls(schema, [batch])
