from typing import Any, Iterator

import pyarrow as pa

from forge.datatypes import Schema, RecordBatch, ArrowVector, to_arrow_type
from .plan import PhysicalPlan
from .expressions import PhysicalExpr, _to_arrow_array


class HashJoinExec(PhysicalPlan):
    def __init__(
        self,
        left: PhysicalPlan,
        right: PhysicalPlan,
        left_keys: list[PhysicalExpr],
        right_keys: list[PhysicalExpr],
        join_type: str,
        schema: Schema,
    ) -> None:
        self._left: PhysicalPlan = left
        self._right: PhysicalPlan = right
        self._left_keys: list[PhysicalExpr] = left_keys
        self._right_keys: list[PhysicalExpr] = right_keys
        self._join_type: str = join_type
        self._schema: Schema = schema

    def schema(self) -> Schema:
        return self._schema

    def execute(self) -> Iterator[RecordBatch]:
        left_schema = self._left.schema()
        right_schema = self._right.schema()
        left_col_count = len(left_schema)
        right_col_count = len(right_schema)

        # Build phase: hash all left rows
        hash_table: dict[tuple[Any, ...], list[list[Any]]] = {}
        matched_left_keys: set[tuple[Any, ...]] | None = None
        if self._join_type == "left":
            matched_left_keys = set()
        all_left_rows: list[list[Any]] = []

        for batch in self._left.execute():
            key_cols = [expr.evaluate(batch) for expr in self._left_keys]
            for row_idx in range(batch.row_count):
                key = tuple(col.get_value(row_idx) for col in key_cols)
                row = [batch.column(c).get_value(row_idx) for c in range(left_col_count)]
                if key not in hash_table:
                    hash_table[key] = []
                hash_table[key].append(row)
                if self._join_type == "left":
                    all_left_rows.append(row)

        # Probe phase: stream right side
        for batch in self._right.execute():
            key_cols = [expr.evaluate(batch) for expr in self._right_keys]
            output_columns: list[list[Any]] = [[] for _ in range(left_col_count + right_col_count)]

            for row_idx in range(batch.row_count):
                key = tuple(col.get_value(row_idx) for col in key_cols)
                right_row = [batch.column(c).get_value(row_idx) for c in range(right_col_count)]

                if key in hash_table:
                    if matched_left_keys is not None:
                        matched_left_keys.add(key)
                    for left_row in hash_table[key]:
                        for c in range(left_col_count):
                            output_columns[c].append(left_row[c])
                        for c in range(right_col_count):
                            output_columns[left_col_count + c].append(right_row[c])

            if any(len(col) > 0 for col in output_columns):
                arrays = [
                    pa.array(output_columns[i], type=to_arrow_type(self._schema[i].data_type))
                    for i in range(left_col_count + right_col_count)
                ]
                arrow_schema = pa.schema([
                    pa.field(self._schema[i].name, to_arrow_type(self._schema[i].data_type))
                    for i in range(left_col_count + right_col_count)
                ])
                yield RecordBatch.from_arrow(pa.RecordBatch.from_arrays(arrays, schema=arrow_schema))

        # Emit unmatched left rows for left join
        if self._join_type == "left" and matched_left_keys is not None:
            output_columns = [[] for _ in range(left_col_count + right_col_count)]
            for key, rows in hash_table.items():
                if key not in matched_left_keys:
                    for left_row in rows:
                        for c in range(left_col_count):
                            output_columns[c].append(left_row[c])
                        for c in range(right_col_count):
                            output_columns[left_col_count + c].append(None)

            if any(len(col) > 0 for col in output_columns):
                arrays = [
                    pa.array(output_columns[i], type=to_arrow_type(self._schema[i].data_type))
                    for i in range(left_col_count + right_col_count)
                ]
                arrow_schema = pa.schema([
                    pa.field(self._schema[i].name, to_arrow_type(self._schema[i].data_type))
                    for i in range(left_col_count + right_col_count)
                ])
                yield RecordBatch.from_arrow(pa.RecordBatch.from_arrays(arrays, schema=arrow_schema))

    def children(self) -> list[PhysicalPlan]:
        return [self._left, self._right]

    def __str__(self) -> str:
        left_keys_str = ", ".join(str(k) for k in self._left_keys)
        right_keys_str = ", ".join(str(k) for k in self._right_keys)
        return f"HashJoinExec({self._join_type}, left_keys=[{left_keys_str}], right_keys=[{right_keys_str}])"
