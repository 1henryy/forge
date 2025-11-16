from __future__ import annotations

from forge.logicalplan.expressions import (
    LogicalExpr,
    Column,
    ColumnIndex,
    BinaryExpr,
    AggregateExpr,
    ScalarFunction,
    Cast,
    Alias,
    Not,
)
from forge.logicalplan.plan import (
    LogicalPlan,
    Scan,
    Projection,
    Selection,
    Aggregate,
    Join,
    Sort,
    Limit,
)

from .optimizer import OptimizerRule


def _collect_columns(expr: LogicalExpr) -> set[str]:
    if isinstance(expr, Column):
        return {expr.name}
    if isinstance(expr, ColumnIndex):
        return set()
    if isinstance(expr, BinaryExpr):
        return _collect_columns(expr.left) | _collect_columns(expr.right)
    if isinstance(expr, Not):
        return _collect_columns(expr.expr)
    if isinstance(expr, AggregateExpr):
        return _collect_columns(expr.expr)
    if isinstance(expr, ScalarFunction):
        result: set[str] = set()
        for arg in expr.args:
            result |= _collect_columns(arg)
        return result
    if isinstance(expr, Cast):
        return _collect_columns(expr.expr)
    if isinstance(expr, Alias):
        return _collect_columns(expr.expr)
    return set()


def _collect_all_columns(exprs: list[LogicalExpr]) -> set[str]:
    result: set[str] = set()
    for expr in exprs:
        result |= _collect_columns(expr)
    return result


class ProjectionPushdown(OptimizerRule):
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        needed: set[str] = set()
        for field in plan.schema().fields:
            needed.add(field.name)
        return self._push_down(plan, needed)

    def _push_down(self, plan: LogicalPlan, needed: set[str]) -> LogicalPlan:
        if isinstance(plan, Scan):
            return self._push_to_scan(plan, needed)
        if isinstance(plan, Projection):
            return self._push_projection(plan, needed)
        if isinstance(plan, Selection):
            return self._push_selection(plan, needed)
        if isinstance(plan, Aggregate):
            return self._push_aggregate(plan, needed)
        if isinstance(plan, Join):
            return self._push_join(plan, needed)
        if isinstance(plan, Sort):
            return self._push_sort(plan, needed)
        if isinstance(plan, Limit):
            return Limit(self._push_down(plan.input, needed), plan.limit)
        return plan

    def _push_to_scan(self, scan: Scan, needed: set[str]) -> Scan:
        if not needed:
            return scan
        source_schema = scan.source.schema()
        indices: list[int] = []
        for i, field in enumerate(source_schema.fields):
            if field.name in needed:
                indices.append(i)
        if not indices:
            return scan
        if scan.projection is not None:
            existing = set(scan.projection)
            indices = sorted(existing & set(indices))
            if not indices:
                indices = sorted(existing)
        return Scan(scan.source, scan.name, indices if indices else None)

    def _push_projection(self, proj: Projection, needed: set[str]) -> LogicalPlan:
        child_needed = _collect_all_columns(proj.exprs)
        new_input = self._push_down(proj.input, child_needed)
        return Projection(new_input, proj.exprs)

    def _push_selection(self, sel: Selection, needed: set[str]) -> LogicalPlan:
        filter_cols = _collect_columns(sel.expr)
        child_needed = needed | filter_cols
        new_input = self._push_down(sel.input, child_needed)
        return Selection(new_input, sel.expr)

    def _push_aggregate(self, agg: Aggregate, needed: set[str]) -> LogicalPlan:
        child_needed: set[str] = set()
        child_needed |= _collect_all_columns(agg.group_exprs)
        child_needed |= _collect_all_columns(agg.agg_exprs)
        new_input = self._push_down(agg.input, child_needed)
        return Aggregate(new_input, agg.group_exprs, agg.agg_exprs)

    def _push_join(self, join: Join, needed: set[str]) -> LogicalPlan:
        left_schema_names = {f.name for f in join.left.schema().fields}
        right_schema_names = {f.name for f in join.right.schema().fields}

        left_needed: set[str] = set()
        right_needed: set[str] = set()

        for col_name in needed:
            if col_name in left_schema_names:
                left_needed.add(col_name)
            if col_name in right_schema_names:
                right_needed.add(col_name)

        for left_expr, right_expr in join.on:
            left_needed |= _collect_columns(left_expr)
            right_needed |= _collect_columns(right_expr)

        new_left = self._push_down(join.left, left_needed)
        new_right = self._push_down(join.right, right_needed)
        return Join(new_left, new_right, join.join_type, join.on)

    def _push_sort(self, sort: Sort, needed: set[str]) -> LogicalPlan:
        sort_cols: set[str] = set()
        for expr, _ in sort.sort_exprs:
            sort_cols |= _collect_columns(expr)
        child_needed = needed | sort_cols
        new_input = self._push_down(sort.input, child_needed)
        return Sort(new_input, sort.sort_exprs)
