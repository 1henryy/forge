from typing import Iterator

from forge.datatypes import Schema, RecordBatch
from forge.datasources.datasource import DataSource
from .plan import PhysicalPlan


class ScanExec(PhysicalPlan):
    def __init__(self, source: DataSource, projection: list[int] | None = None) -> None:
        self._source: DataSource = source
        self._projection: list[int] | None = projection

    def schema(self) -> Schema:
        if self._projection is not None:
            return self._source.schema().project(self._projection)
        return self._source.schema()

    def execute(self) -> Iterator[RecordBatch]:
        return self._source.scan(self._projection)

    def children(self) -> list[PhysicalPlan]:
        return []

    def __str__(self) -> str:
        proj = self._projection if self._projection is not None else "all"
        return f"ScanExec(projection={proj})"
