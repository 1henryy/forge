# __MARKER_0__
# __MARKER_1__
from __future__ import annotations

from abc import ABC, abstractmethod

from forge.datasources import DataSource
from forge.datatypes import Field, Schema

from .expressions import AggregateExpr, LogicalExpr


class LogicalPlan(ABC):
    @abstractmethod
    def schema(self) -> Schema:
        ...

    @abstractmethod
    def children(self) -> list[LogicalPlan]:
        ...

    @abstractmethod
    def __str__(self) -> str:
        ...

    def format(self, indent: int = 0) -> str:
        result = "  " * indent + str(self) + "\n"
        for child in self.children():
            result += child.format(indent + 1)
        return result


class Scan(LogicalPlan):
    def __init__(self, source: DataSource, name: str, projection: list[int] | None = None) -> None:
        self.source = source
        self.name = name
        self.projection = projection

    def schema(self) -> Schema:
        base = self.source.schema()
        if self.projection is not None:
            return base.project(self.projection)
        return base

    def children(self) -> list[LogicalPlan]:
        return []

    def __str__(self) -> str:
        if self.projection is not None:
            cols = [self.source.schema()[i].name for i in self.projection]
            return f"Scan: {self.name}; projection={cols}"
        return f"Scan: {self.name}; projection=None"


class Projection(LogicalPlan):
    def __init__(self, input: LogicalPlan, exprs: list[LogicalExpr]) -> None:
        self.input = input
        self.exprs = exprs

    def schema(self) -> Schema:
        input_schema = self.input.schema()
        return Schema([e.to_field(input_schema) for e in self.exprs])

    def children(self) -> list[LogicalPlan]:
        return [self.input]

    def __str__(self) -> str:
        expr_strs = ", ".join(str(e) for e in self.exprs)
        return f"Projection: {expr_strs}"


class Selection(LogicalPlan):
    def __init__(self, input: LogicalPlan, expr: LogicalExpr) -> None:
        self.input = input
        self.expr = expr

    def schema(self) -> Schema:
        return self.input.schema()

    def children(self) -> list[LogicalPlan]:
        return [self.input]

    def __str__(self) -> str:
        return f"Selection: {self.expr}"


class Aggregate(LogicalPlan):
    def __init__(
        self,
        input: LogicalPlan,
        group_exprs: list[LogicalExpr],
        agg_exprs: list[AggregateExpr],
    ) -> None:
        self.input = input
        self.group_exprs = group_exprs
        self.agg_exprs = agg_exprs

    def schema(self) -> Schema:
        input_schema = self.input.schema()
        fields: list[Field] = []
        for e in self.group_exprs:
            fields.append(e.to_field(input_schema))
        for e in self.agg_exprs:
            fields.append(e.to_field(input_schema))
        return Schema(fields)

    def children(self) -> list[LogicalPlan]:
        return [self.input]

    def __str__(self) -> str:
        groups = ", ".join(str(e) for e in self.group_exprs)
        aggs = ", ".join(str(e) for e in self.agg_exprs)
        return f"Aggregate: group=[{groups}], agg=[{aggs}]"


class Join(LogicalPlan):
    def __init__(
        self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: str,
        on: list[tuple[LogicalExpr, LogicalExpr]],
    ) -> None:
        self.left = left
        self.right = right
        self.join_type = join_type
        self.on = on

    def schema(self) -> Schema:
        left_fields = self.left.schema().fields
        right_fields = self.right.schema().fields
        return Schema(left_fields + right_fields)

    def children(self) -> list[LogicalPlan]:
        return [self.left, self.right]

    def __str__(self) -> str:
        on_strs = ", ".join(f"{l} = {r}" for l, r in self.on)
        return f"Join: type={self.join_type}, on=[{on_strs}]"


class Sort(LogicalPlan):
    def __init__(self, input: LogicalPlan, sort_exprs: list[tuple[LogicalExpr, bool]]) -> None:
        self.input = input
        self.sort_exprs = sort_exprs

    def schema(self) -> Schema:
        return self.input.schema()

    def children(self) -> list[LogicalPlan]:
        return [self.input]

    def __str__(self) -> str:
        parts = []
        for expr, asc in self.sort_exprs:
            direction = "ASC" if asc else "DESC"
            parts.append(f"{expr} {direction}")
        return f"Sort: {', '.join(parts)}"


class Limit(LogicalPlan):
    def __init__(self, input: LogicalPlan, limit: int) -> None:
        self.input = input
        self.limit = limit

    def schema(self) -> Schema:
        return self.input.schema()

    def children(self) -> list[LogicalPlan]:
        return [self.input]

    def __str__(self) -> str:
        return f"Limit: {self.limit}"
