from typing import Iterator

from forge.datatypes import Schema, RecordBatch, ColumnVector
from .plan import PhysicalPlan
from .expressions import PhysicalExpr


class ProjectionExec(PhysicalPlan):
    def __init__(self, input_plan: PhysicalPlan, schema: Schema, exprs: list[PhysicalExpr]) -> None:
        self._input: PhysicalPlan = input_plan
        self._schema: Schema = schema
        self._exprs: list[PhysicalExpr] = exprs

    def schema(self) -> Schema:
        return self._schema

    def execute(self) -> Iterator[RecordBatch]:
        for batch in self._input.execute():
            columns: list[ColumnVector] = [expr.evaluate(batch) for expr in self._exprs]
            yield RecordBatch(self._schema, columns)

    def children(self) -> list[PhysicalPlan]:
        return [self._input]

    def __str__(self) -> str:
        exprs_str = ", ".join(str(e) for e in self._exprs)
        return f"ProjectionExec({exprs_str})"
