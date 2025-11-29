from typing import Iterator

from forge.datatypes import Schema, RecordBatch
from .plan import PhysicalPlan


class LimitExec(PhysicalPlan):
    def __init__(self, input_plan: PhysicalPlan, limit: int) -> None:
        self._input: PhysicalPlan = input_plan
        self._limit: int = limit

    def schema(self) -> Schema:
        return self._input.schema()

    def execute(self) -> Iterator[RecordBatch]:
        remaining: int = self._limit
        for batch in self._input.execute():
            if remaining <= 0:
                break
            if batch.row_count <= remaining:
                remaining -= batch.row_count
                yield batch
            else:
                arrow_batch = batch.to_arrow()
                sliced = arrow_batch.slice(0, remaining)
                yield RecordBatch.from_arrow(sliced)
                remaining = 0

    def children(self) -> list[PhysicalPlan]:
        return [self._input]

    def __str__(self) -> str:
        return f"LimitExec(limit={self._limit})"
