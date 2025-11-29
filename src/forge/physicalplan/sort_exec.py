from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc

from forge.datatypes import Schema, RecordBatch
from .plan import PhysicalPlan
from .expressions import PhysicalExpr, _to_arrow_array


class SortExec(PhysicalPlan):
    def __init__(
        self,
        input_plan: PhysicalPlan,
        sort_exprs: list[tuple[PhysicalExpr, bool]],
    ) -> None:
        self._input: PhysicalPlan = input_plan
        self._sort_exprs: list[tuple[PhysicalExpr, bool]] = sort_exprs

    def schema(self) -> Schema:
        return self._input.schema()

    def execute(self) -> Iterator[RecordBatch]:
        batches: list[pa.RecordBatch] = []
        for batch in self._input.execute():
            batches.append(batch.to_arrow())

        if not batches:
            return

        table = pa.Table.from_batches(batches)

        sort_keys_columns: list[pa.Array] = []
        sort_orders: list[str] = []

        combined_batch = RecordBatch.from_arrow(table.combine_chunks().to_batches()[0])

        sort_key_names: list[tuple[str, str]] = []
        extra_columns: dict[str, pa.Array] = {}
        for i, (expr, ascending) in enumerate(self._sort_exprs):
            col = expr.evaluate(combined_batch)
            arr = _to_arrow_array(col)
            col_name = f"__sort_key_{i}"
            extra_columns[col_name] = arr
            order = "ascending" if ascending else "descending"
            sort_key_names.append((col_name, order))

        for name, arr in extra_columns.items():
            table = table.append_column(name, arr)

        indices = pc.sort_indices(table, sort_keys=sort_key_names)
        sorted_table = table.take(indices)

        # Remove sort key columns
        for name in extra_columns:
            col_idx = sorted_table.schema.get_field_index(name)
            sorted_table = sorted_table.remove_column(col_idx)

        for arrow_batch in sorted_table.to_batches():
            yield RecordBatch.from_arrow(arrow_batch)

    def children(self) -> list[PhysicalPlan]:
        return [self._input]

    def __str__(self) -> str:
        exprs_str = ", ".join(
            f"{expr} {'ASC' if asc else 'DESC'}"
            for expr, asc in self._sort_exprs
        )
        return f"SortExec({exprs_str})"
