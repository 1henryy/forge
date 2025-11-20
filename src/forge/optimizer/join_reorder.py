from __future__ import annotations

from forge.logicalplan.expressions import LogicalExpr, Column, BinaryExpr
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


def _has_equality_predicate(on: list[tuple[LogicalExpr, LogicalExpr]]) -> bool:
    return len(on) > 0


def _estimate_scan_size(plan: LogicalPlan) -> int:
    if isinstance(plan, Scan):
        schema = plan.source.schema()
        return len(schema)
    if isinstance(plan, Selection):
        return _estimate_scan_size(plan.input)
    if isinstance(plan, Projection):
        return _estimate_scan_size(plan.input)
    return 1000


def _collect_join_inputs(plan: LogicalPlan) -> tuple[
    list[LogicalPlan],
    list[list[tuple[LogicalExpr, LogicalExpr]]],
    list[str],
] | None:
    if not isinstance(plan, Join):
        return None

    if plan.join_type.upper() != "INNER":
        return None

    tables: list[LogicalPlan] = []
    predicates: list[list[tuple[LogicalExpr, LogicalExpr]]] = []
    join_types: list[str] = []

    _flatten_inner_joins(plan, tables, predicates, join_types)
    return tables, predicates, join_types


def _flatten_inner_joins(
    plan: LogicalPlan,
    tables: list[LogicalPlan],
    predicates: list[list[tuple[LogicalExpr, LogicalExpr]]],
    join_types: list[str],
) -> None:
    if isinstance(plan, Join) and plan.join_type.upper() == "INNER":
        _flatten_inner_joins(plan.left, tables, predicates, join_types)
        predicates.append(list(plan.on))
        join_types.append(plan.join_type)
        _flatten_inner_joins(plan.right, tables, predicates, join_types)
    else:
        tables.append(plan)


def _extract_referenced_tables(
    expr: LogicalExpr,
    table_columns: dict[int, set[str]],
) -> set[int]:
    cols = _extract_column_names(expr)
    result: set[int] = set()
    for table_idx, col_names in table_columns.items():
        if cols & col_names:
            result.add(table_idx)
    return result


def _extract_column_names(expr: LogicalExpr) -> set[str]:
    if isinstance(expr, Column):
        return {expr.name}
    if isinstance(expr, BinaryExpr):
        return _extract_column_names(expr.left) | _extract_column_names(expr.right)
    return set()


class JoinReorder(OptimizerRule):
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        return self._reorder(plan)

    def _reorder(self, plan: LogicalPlan) -> LogicalPlan:
        if isinstance(plan, Join):
            new_left = self._reorder(plan.left)
            new_right = self._reorder(plan.right)
            reordered_join = Join(new_left, new_right, plan.join_type, plan.on)
            return self._try_reorder_join(reordered_join)

        if isinstance(plan, Projection):
            return Projection(self._reorder(plan.input), plan.exprs)
        if isinstance(plan, Selection):
            return Selection(self._reorder(plan.input), plan.expr)
        if isinstance(plan, Aggregate):
            return Aggregate(
                self._reorder(plan.input),
                plan.group_exprs,
                plan.agg_exprs,
            )
        if isinstance(plan, Sort):
            return Sort(self._reorder(plan.input), plan.sort_exprs)
        if isinstance(plan, Limit):
            return Limit(self._reorder(plan.input), plan.limit)
        return plan

    def _try_reorder_join(self, plan: Join) -> LogicalPlan:
        collected = _collect_join_inputs(plan)
        if collected is None:
            return plan

        tables, predicates, join_types = collected
        if len(tables) <= 2:
            return plan

        table_columns: dict[int, set[str]] = {}
        for i, table in enumerate(tables):
            table_columns[i] = {f.name for f in table.schema().fields}

        all_predicates: list[tuple[LogicalExpr, LogicalExpr]] = []
        for pred_list in predicates:
            all_predicates.extend(pred_list)

        sorted_indices = sorted(
            range(len(tables)),
            key=lambda i: _estimate_scan_size(tables[i]),
        )

        result: LogicalPlan = tables[sorted_indices[0]]
        joined_tables: set[int] = {sorted_indices[0]}

        for idx in sorted_indices[1:]:
            applicable: list[tuple[LogicalExpr, LogicalExpr]] = []
            for left_expr, right_expr in all_predicates:
                left_tables = _extract_referenced_tables(left_expr, table_columns)
                right_tables = _extract_referenced_tables(right_expr, table_columns)
                all_refs = left_tables | right_tables
                needed = joined_tables | {idx}
                if all_refs and all_refs.issubset(needed) and idx in all_refs:
                    applicable.append((left_expr, right_expr))

            for pred in applicable:
                all_predicates.remove(pred)

            join_type = join_types[0] if join_types else "INNER"
            result = Join(result, tables[idx], join_type, applicable)
            joined_tables.add(idx)

        if all_predicates:
            result = Join(
                result,
                result,
                "INNER",
                all_predicates,
            )

        return result
