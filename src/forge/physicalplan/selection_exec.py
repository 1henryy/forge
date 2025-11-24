from typing import Iterator

import pyarrow as pa

from forge.datatypes import Schema, RecordBatch
from .plan import PhysicalPlan
from .expressions import PhysicalExpr, _to_arrow_array


class SelectionExec(PhysicalPlan):
    def __init__(self, input_plan: PhysicalPlan, expr: PhysicalExpr) -> None:
        self._input: PhysicalPlan = input_plan
        self._expr: PhysicalExpr = expr

    def schema(self) -> Schema:
        return self._input.schema()

    def execute(self) -> Iterator[RecordBatch]:
        for batch in self._input.execute():
            filter_result = self._expr.evaluate(batch)
            filter_arr = _to_arrow_array(filter_result)
            arrow_batch = batch.to_arrow()
            filtered = arrow_batch.filter(filter_arr)
            if filtered.num_rows > 0:
                yield RecordBatch.from_arrow(filtered)

    def children(self) -> list[PhysicalPlan]:
        return [self._input]

    def __str__(self) -> str:
        return f"SelectionExec({self._expr})"
