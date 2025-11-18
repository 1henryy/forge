# __MARKER_0__
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
    And,
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


def _extract_columns(expr: LogicalExpr) -> set[str]:
    if isinstance(expr, Column):
        return {expr.name}
    if isinstance(expr, ColumnIndex):
        return set()
    if isinstance(expr, BinaryExpr):
        return _extract_columns(expr.left) | _extract_columns(expr.right)
    if isinstance(expr, Not):
        return _extract_columns(expr.expr)
    if isinstance(expr, AggregateExpr):
        return _extract_columns(expr.expr)
    if isinstance(expr, ScalarFunction):
        result: set[str] = set()
        for arg in expr.args:
            result |= _extract_columns(arg)
        return result
    if isinstance(expr, Cast):
        return _extract_columns(expr.expr)
    if isinstance(expr, Alias):
        return _extract_columns(expr.expr)
    return set()


def _schema_field_names(plan: LogicalPlan) -> set[str]:
    return {f.name for f in plan.schema().fields}


def _split_conjunction(expr: LogicalExpr) -> list[LogicalExpr]:
    if isinstance(expr, And):
        return _split_conjunction(expr.left) + _split_conjunction(expr.right)
    return [expr]


def _combine_conjunction(exprs: list[LogicalExpr]) -> LogicalExpr | None:
    if not exprs:
        return None
    result = exprs[0]
    for e in exprs[1:]:
        result = And(result, e)
    return result


class FilterPushdown(OptimizerRule):
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        return self._push_down(plan)

    def _push_down(self, plan: LogicalPlan) -> LogicalPlan:
        if isinstance(plan, Selection):
            return self._push_selection(plan)
        if isinstance(plan, Projection):
            return Projection(self._push_down(plan.input), plan.exprs)
        if isinstance(plan, Join):
            return Join(
                self._push_down(plan.left),
                self._push_down(plan.right),
                plan.join_type,
                plan.on,
            )
        if isinstance(plan, Aggregate):
            return Aggregate(
                self._push_down(plan.input),
                plan.group_exprs,
                plan.agg_exprs,
            )
        if isinstance(plan, Sort):
            return Sort(self._push_down(plan.input), plan.sort_exprs)
        if isinstance(plan, Limit):
            return Limit(self._push_down(plan.input), plan.limit)
        return plan

    def _push_selection(self, sel: Selection) -> LogicalPlan:
        child = sel.input
        filter_expr = sel.expr

        if isinstance(child, Projection):
            input_cols = _schema_field_names(child.input)
            referenced = _extract_columns(filter_expr)
            if referenced.issubset(input_cols):
                new_input = self._push_down(Selection(child.input, filter_expr))
                return Projection(new_input, child.exprs)
            return Selection(Projection(self._push_down(child.input), child.exprs), filter_expr)

        if isinstance(child, Join):
            left_cols = _schema_field_names(child.left)
            right_cols = _schema_field_names(child.right)
            conjuncts = _split_conjunction(filter_expr)

            left_filters: list[LogicalExpr] = []
            right_filters: list[LogicalExpr] = []
            remaining: list[LogicalExpr] = []

            for conj in conjuncts:
                cols = _extract_columns(conj)
                if cols and cols.issubset(left_cols) and not cols.issubset(right_cols):
                    left_filters.append(conj)
                elif cols and cols.issubset(right_cols) and not cols.issubset(left_cols):
                    right_filters.append(conj)
                else:
                    remaining.append(conj)

            new_left: LogicalPlan = child.left
            if left_filters:
                left_pred = _combine_conjunction(left_filters)
                assert left_pred is not None
                new_left = Selection(new_left, left_pred)
            new_left = self._push_down(new_left)

            new_right: LogicalPlan = child.right
            if right_filters:
                right_pred = _combine_conjunction(right_filters)
                assert right_pred is not None
                new_right = Selection(new_right, right_pred)
            new_right = self._push_down(new_right)

            new_join: LogicalPlan = Join(new_left, new_right, child.join_type, child.on)
            remaining_pred = _combine_conjunction(remaining)
            if remaining_pred is not None:
                return Selection(new_join, remaining_pred)
            return new_join

        if isinstance(child, Selection):
            combined = And(child.expr, filter_expr)
            return self._push_down(Selection(child.input, combined))

        if isinstance(child, Sort):
            new_input = self._push_down(Selection(child.input, filter_expr))
            return Sort(new_input, child.sort_exprs)

        if isinstance(child, Limit):
            return Selection(Limit(self._push_down(child.input), child.limit), filter_expr)

        if isinstance(child, Scan):
            return Selection(child, filter_expr)

        return Selection(self._push_down(child), filter_expr)
