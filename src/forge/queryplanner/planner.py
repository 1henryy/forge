from __future__ import annotations

from forge.logicalplan.plan import (
    LogicalPlan, Scan, Projection, Selection, Aggregate, Join, Sort, Limit,
)
from forge.logicalplan import expressions as lexpr
from forge.physicalplan import expressions as pexpr
from forge.physicalplan.scan_exec import ScanExec
from forge.physicalplan.projection_exec import ProjectionExec
from forge.physicalplan.selection_exec import SelectionExec
from forge.physicalplan.hash_aggregate_exec import HashAggregateExec
from forge.physicalplan.hash_join_exec import HashJoinExec
from forge.physicalplan.sort_exec import SortExec
from forge.physicalplan.limit_exec import LimitExec
from forge.physicalplan.plan import PhysicalPlan

_OP_MAP = {
    "=": "equal",
    "!=": "not_equal",
    "<": "less",
    ">": "greater",
    "<=": "less_equal",
    ">=": "greater_equal",
    "+": "add",
    "-": "subtract",
    "*": "multiply",
    "/": "divide",
    "AND": "and",
    "OR": "or",
}


class QueryPlanner:
    def create_physical_plan(self, logical: LogicalPlan) -> PhysicalPlan:
        if isinstance(logical, Scan):
            return ScanExec(logical.source, logical.projection)
        elif isinstance(logical, Projection):
            input_plan = self.create_physical_plan(logical.input)
            physical_exprs = [
                self.create_physical_expr(e, logical.input) for e in logical.exprs
            ]
            return ProjectionExec(input_plan, logical.schema(), physical_exprs)
        elif isinstance(logical, Selection):
            input_plan = self.create_physical_plan(logical.input)
            filter_expr = self.create_physical_expr(logical.expr, logical.input)
            return SelectionExec(input_plan, filter_expr)
        elif isinstance(logical, Aggregate):
            input_plan = self.create_physical_plan(logical.input)
            group_exprs = [
                self.create_physical_expr(e, logical.input) for e in logical.group_exprs
            ]
            agg_exprs = [
                self._create_agg_expr(e, logical.input) for e in logical.agg_exprs
            ]
            return HashAggregateExec(input_plan, group_exprs, agg_exprs, logical.schema())
        elif isinstance(logical, Join):
            left_plan = self.create_physical_plan(logical.left)
            right_plan = self.create_physical_plan(logical.right)
            left_keys = [self.create_physical_expr(p[0], logical.left) for p in logical.on]
            right_keys = [self.create_physical_expr(p[1], logical.right) for p in logical.on]
            return HashJoinExec(
                left_plan, right_plan, left_keys, right_keys,
                logical.join_type, logical.schema(),
            )
        elif isinstance(logical, Sort):
            input_plan = self.create_physical_plan(logical.input)
            sort_exprs = [
                (self.create_physical_expr(e, logical.input), asc)
                for e, asc in logical.sort_exprs
            ]
            return SortExec(input_plan, sort_exprs)
        elif isinstance(logical, Limit):
            input_plan = self.create_physical_plan(logical.input)
            return LimitExec(input_plan, logical.limit)
        else:
            raise ValueError(f"Unsupported logical plan: {type(logical).__name__}")

    def create_physical_expr(
        self, expr: lexpr.LogicalExpr, input_plan: LogicalPlan,
    ) -> pexpr.PhysicalExpr:
        schema = input_plan.schema()
        if isinstance(expr, lexpr.Column):
            idx = schema.field_index(expr.name)
            return pexpr.ColumnExpr(idx)
        elif isinstance(expr, lexpr.ColumnIndex):
            return pexpr.ColumnExpr(expr.index)
        elif isinstance(expr, lexpr.LiteralLong):
            return pexpr.LiteralLongExpr(expr.value)
        elif isinstance(expr, lexpr.LiteralDouble):
            return pexpr.LiteralDoubleExpr(expr.value)
        elif isinstance(expr, lexpr.LiteralString):
            return pexpr.LiteralStringExpr(expr.value)
        elif isinstance(expr, lexpr.LiteralBoolean):
            return pexpr.LiteralBoolExpr(expr.value)
        elif isinstance(expr, lexpr.BinaryExpr):
            left = self.create_physical_expr(expr.left, input_plan)
            right = self.create_physical_expr(expr.right, input_plan)
            op = _OP_MAP.get(expr.op, expr.op)
            return pexpr.BinaryExpr(left, op, right)
        elif isinstance(expr, lexpr.Not):
            return pexpr.NotExpr(self.create_physical_expr(expr.expr, input_plan))
        elif isinstance(expr, lexpr.Cast):
            return pexpr.CastExpr(
                self.create_physical_expr(expr.expr, input_plan), expr.data_type,
            )
        elif isinstance(expr, lexpr.Alias):
            return self.create_physical_expr(expr.expr, input_plan)
        elif isinstance(expr, lexpr.AggregateExpr):
            return self.create_physical_expr(expr.expr, input_plan)
        else:
            raise ValueError(f"Unsupported expression: {type(expr).__name__}")

    def _create_agg_expr(
        self, expr: lexpr.AggregateExpr, input_plan: LogicalPlan,
    ) -> pexpr.AggregateExpr:
        physical_input = self.create_physical_expr(expr.expr, input_plan)
        name = expr.name.upper()
        dispatch = {
            "SUM": pexpr.SumExpr,
            "COUNT": pexpr.CountExpr,
            "MIN": pexpr.MinExpr,
            "MAX": pexpr.MaxExpr,
            "AVG": pexpr.AvgExpr,
        }
        cls = dispatch.get(name)
        if cls is None:
            raise ValueError(f"Unsupported aggregate: {name}")
        return cls(physical_input)
