from __future__ import annotations

from forge.logicalplan.expressions import (
    LogicalExpr,
    LiteralLong,
    LiteralDouble,
    LiteralString,
    LiteralBoolean,
    BinaryExpr,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Not,
    AggregateExpr,
    ScalarFunction,
    Cast,
    Alias,
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

_ARITHMETIC_OPS: dict[str, type[BinaryExpr]] = {
    "+": Add,
    "-": Subtract,
    "*": Multiply,
    "/": Divide,
}

_COMPARISON_OPS: dict[str, type[BinaryExpr]] = {
    "=": Eq,
    "!=": Neq,
    "<": Lt,
    "<=": LtEq,
    ">": Gt,
    ">=": GtEq,
}


def _is_literal_numeric(expr: LogicalExpr) -> bool:
    return isinstance(expr, (LiteralLong, LiteralDouble))


def _get_numeric_value(expr: LiteralLong | LiteralDouble) -> int | float:
    return expr.value


def _eval_arithmetic(op: str, left_val: int | float, right_val: int | float) -> int | float | None:
    if op == "+":
        return left_val + right_val
    if op == "-":
        return left_val - right_val
    if op == "*":
        return left_val * right_val
    if op == "/":
        if right_val == 0:
            return None
        return left_val / right_val
    return None


def _eval_comparison(op: str, left_val: int | float, right_val: int | float) -> bool | None:
    if op == "=":
        return left_val == right_val
    if op == "!=":
        return left_val != right_val
    if op == "<":
        return left_val < right_val
    if op == "<=":
        return left_val <= right_val
    if op == ">":
        return left_val > right_val
    if op == ">=":
        return left_val >= right_val
    return None


def _make_numeric_literal(value: int | float) -> LogicalExpr:
    if isinstance(value, int):
        return LiteralLong(value)
    return LiteralDouble(value)


def _fold_expr(expr: LogicalExpr) -> LogicalExpr:
    if isinstance(expr, BinaryExpr):
        left = _fold_expr(expr.left)
        right = _fold_expr(expr.right)

        if expr.op in ("AND", "OR"):
            return _fold_boolean_op(expr.op, left, right)

        if _is_literal_numeric(left) and _is_literal_numeric(right):
            left_val = _get_numeric_value(left)  # type: ignore[arg-type]
            right_val = _get_numeric_value(right)  # type: ignore[arg-type]

            if expr.op in _ARITHMETIC_OPS:
                result = _eval_arithmetic(expr.op, left_val, right_val)
                if result is not None:
                    if isinstance(left, LiteralLong) and isinstance(right, LiteralLong) and expr.op != "/":
                        return LiteralLong(int(result))
                    return _make_numeric_literal(result)

            if expr.op in _COMPARISON_OPS:
                result_bool = _eval_comparison(expr.op, left_val, right_val)
                if result_bool is not None:
                    return LiteralBoolean(result_bool)

        if left is not expr.left or right is not expr.right:
            cls = type(expr)
            if cls is BinaryExpr:
                return BinaryExpr(expr.name, expr.op, left, right)
            return cls(left, right)

        return expr

    if isinstance(expr, Not):
        inner = _fold_expr(expr.expr)
        if isinstance(inner, LiteralBoolean):
            return LiteralBoolean(not inner.value)
        if inner is not expr.expr:
            return Not(inner)
        return expr

    if isinstance(expr, AggregateExpr):
        folded = _fold_expr(expr.expr)
        if folded is not expr.expr:
            return AggregateExpr(expr.name, folded)
        return expr

    if isinstance(expr, ScalarFunction):
        new_args = [_fold_expr(a) for a in expr.args]
        if any(n is not o for n, o in zip(new_args, expr.args)):
            return ScalarFunction(expr.name, new_args, expr.return_type)
        return expr

    if isinstance(expr, Cast):
        folded = _fold_expr(expr.expr)
        if folded is not expr.expr:
            return Cast(folded, expr.data_type)
        return expr

    if isinstance(expr, Alias):
        folded = _fold_expr(expr.expr)
        if folded is not expr.expr:
            return Alias(folded, expr.alias)
        return expr

    return expr


def _fold_boolean_op(op: str, left: LogicalExpr, right: LogicalExpr) -> LogicalExpr:
    if op == "AND":
        if isinstance(left, LiteralBoolean):
            if left.value:
                return right
            return LiteralBoolean(False)
        if isinstance(right, LiteralBoolean):
            if right.value:
                return left
            return LiteralBoolean(False)
        return And(left, right)

    if op == "OR":
        if isinstance(left, LiteralBoolean):
            if left.value:
                return LiteralBoolean(True)
            return right
        if isinstance(right, LiteralBoolean):
            if right.value:
                return LiteralBoolean(True)
            return left
        return Or(left, right)

    return BinaryExpr(f"{left} {op} {right}", op, left, right)


def _fold_exprs(exprs: list[LogicalExpr]) -> list[LogicalExpr]:
    return [_fold_expr(e) for e in exprs]


class ConstantFolding(OptimizerRule):
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        return self._fold_plan(plan)

    def _fold_plan(self, plan: LogicalPlan) -> LogicalPlan:
        if isinstance(plan, Scan):
            return plan

        if isinstance(plan, Projection):
            new_input = self._fold_plan(plan.input)
            new_exprs = _fold_exprs(plan.exprs)
            return Projection(new_input, new_exprs)

        if isinstance(plan, Selection):
            new_input = self._fold_plan(plan.input)
            new_expr = _fold_expr(plan.expr)
            if isinstance(new_expr, LiteralBoolean) and new_expr.value:
                return new_input
            return Selection(new_input, new_expr)

        if isinstance(plan, Aggregate):
            new_input = self._fold_plan(plan.input)
            new_groups = _fold_exprs(plan.group_exprs)
            new_aggs: list[AggregateExpr] = []
            for ae in plan.agg_exprs:
                folded = _fold_expr(ae)
                if isinstance(folded, AggregateExpr):
                    new_aggs.append(folded)
                else:
                    new_aggs.append(ae)
            return Aggregate(new_input, new_groups, new_aggs)

        if isinstance(plan, Join):
            new_left = self._fold_plan(plan.left)
            new_right = self._fold_plan(plan.right)
            new_on: list[tuple[LogicalExpr, LogicalExpr]] = [
                (_fold_expr(l), _fold_expr(r)) for l, r in plan.on
            ]
            return Join(new_left, new_right, plan.join_type, new_on)

        if isinstance(plan, Sort):
            new_input = self._fold_plan(plan.input)
            new_sort: list[tuple[LogicalExpr, bool]] = [
                (_fold_expr(e), asc) for e, asc in plan.sort_exprs
            ]
            return Sort(new_input, new_sort)

        if isinstance(plan, Limit):
            return Limit(self._fold_plan(plan.input), plan.limit)

        return plan
