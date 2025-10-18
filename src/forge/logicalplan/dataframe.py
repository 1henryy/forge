from __future__ import annotations

from forge.datatypes import Schema

from .expressions import AggregateExpr, LogicalExpr
from .plan import Aggregate, Limit, LogicalPlan, Projection, Selection, Sort


class DataFrame:
    def __init__(self, plan: LogicalPlan) -> None:
        self.plan = plan

    def project(self, exprs: list[LogicalExpr]) -> DataFrame:
        return DataFrame(Projection(self.plan, exprs))

    def filter(self, expr: LogicalExpr) -> DataFrame:
        return DataFrame(Selection(self.plan, expr))

    def aggregate(self, group_by: list[LogicalExpr], aggs: list[AggregateExpr]) -> DataFrame:
        return DataFrame(Aggregate(self.plan, group_by, aggs))

    def sort(self, exprs: list[tuple[LogicalExpr, bool]]) -> DataFrame:
        return DataFrame(Sort(self.plan, exprs))

    def limit(self, n: int) -> DataFrame:
        return DataFrame(Limit(self.plan, n))

    def schema(self) -> Schema:
        return self.plan.schema()

    def logical_plan(self) -> LogicalPlan:
        return self.plan
