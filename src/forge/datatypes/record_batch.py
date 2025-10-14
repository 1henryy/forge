from __future__ import annotations

import pyarrow as pa

from .arrow_vector import ArrowVector
from .column_vector import ColumnVector
from .schema import Field, Schema
from .types import from_arrow_type, to_arrow_type


class RecordBatch:
    def __init__(self, schema: Schema, columns: list[ColumnVector]) -> None:
        self._schema = schema
        self._columns = columns

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def row_count(self) -> int:
        if not self._columns:
            return 0
        return self._columns[0].size

    @property
    def column_count(self) -> int:
        return len(self._columns)

    def column(self, index: int) -> ColumnVector:
        return self._columns[index]

    def field(self, index: int) -> Field:
        return self._schema[index]

    def to_arrow(self) -> pa.RecordBatch:
        arrays: list[pa.Array] = []
        for col in self._columns:
            if isinstance(col, ArrowVector):
                arrays.append(col.to_pyarrow())
            else:
                values = [col.get_value(i) for i in range(col.size)]
                arrow_type = to_arrow_type(col.dtype)
                arrays.append(pa.array(values, type=arrow_type))

        fields = [
            pa.field(f.name, to_arrow_type(f.data_type))
            for f in self._schema
        ]
        arrow_schema = pa.schema(fields)
        return pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)

    @staticmethod
    def from_arrow(batch: pa.RecordBatch) -> RecordBatch:
        fields = [
            Field(batch.schema.field(i).name, from_arrow_type(batch.schema.field(i).type))
            for i in range(batch.num_columns)
        ]
        schema = Schema(fields)
        columns: list[ColumnVector] = [
            ArrowVector(batch.column(i)) for i in range(batch.num_columns)
        ]
        return RecordBatch(schema, columns)
