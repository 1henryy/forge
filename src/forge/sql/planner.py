# __MARKER_0__
# __MARKER_1__
from __future__ import annotations

from forge.datasources import DataSource
from forge.datatypes import DataType, Schema
from forge.logicalplan.expressions import (
    Add, AggregateExpr, Alias, And, Cast, Column, Divide, Eq, Gt, GtEq,
    LiteralBoolean, LiteralDouble, LiteralLong, LiteralString, LogicalExpr,
    Lt, LtEq, Multiply, Neq, Not, Or, Subtract,
)
from forge.logicalplan.plan import (
    Aggregate, Join, Limit, LogicalPlan, Projection, Scan, Selection, Sort,
)

from .parser import (
    SQLBinaryOp, SQLBooleanLiteral, SQLCast, SQLColumnRef, SQLExpr,
    SQLFunction, SQLNull, SQLNumber, SQLStar, SQLString, SQLUnaryOp,
    SelectStatement,
)

_AGG_FUNCS = {"SUM", "COUNT", "MIN", "MAX", "AVG"}

_TYPE_MAP: dict[str, DataType] = {
    "INT": DataType.Int32,
    "INTEGER": DataType.Int64,
    "FLOAT": DataType.Float32,
    "DOUBLE": DataType.Float64,
    "VARCHAR": DataType.Utf8,
    "BOOLEAN": DataType.Boolean,
}


class SqlPlanner:
    def __init__(self, tables: dict[str, DataSource]) -> None:
        self._tables = tables

    def create_plan(self, statement: SelectStatement) -> LogicalPlan:
        plan: LogicalPlan | None = None

        if statement.from_table is not None:
            source = self._tables.get(statement.from_table)
            if source is None:
                raise ValueError(f"Unknown table: {statement.from_table}")
            plan = Scan(source, statement.from_table)

        if plan is None:
            raise ValueError("SELECT without FROM is not supported")

        for join_clause in statement.joins:
            right_source = self._tables.get(join_clause.table)
            if right_source is None:
                raise ValueError(f"Unknown table: {join_clause.table}")
            right_plan = Scan(right_source, join_clause.table)

            on_pairs: list[tuple[LogicalExpr, LogicalExpr]] = []
            if join_clause.on_expr is not None:
                combined_schema = Schema(
                    plan.schema().fields + right_plan.schema().fields
                )
                on_pairs = self._extract_join_on(join_clause.on_expr, combined_schema)

            plan = Join(plan, right_plan, join_clause.join_type, on_pairs)

        if statement.where is not None:
            where_expr = self.plan_expr(statement.where, plan.schema())
            plan = Selection(plan, where_expr)

        has_agg = any(
            self._contains_aggregate(col.expr) for col in statement.columns
        )

        if statement.group_by or has_agg:
            group_exprs = [
                self.plan_expr(g, plan.schema()) for g in statement.group_by
            ]
            agg_exprs = self._collect_aggregates(statement, plan.schema())
            plan = Aggregate(plan, group_exprs, agg_exprs)

        if statement.having is not None:
            having_expr = self.plan_expr(statement.having, plan.schema())
            plan = Selection(plan, having_expr)

        proj_exprs = self._plan_projections(statement, plan.schema())
        if proj_exprs:
            plan = Projection(plan, proj_exprs)

        if statement.order_by:
            sort_exprs: list[tuple[LogicalExpr, bool]] = []
            for ob in statement.order_by:
                sort_exprs.append(
                    (self.plan_expr(ob.expr, plan.schema()), ob.ascending)
                )
            plan = Sort(plan, sort_exprs)

        if statement.limit is not None:
            plan = Limit(plan, statement.limit)

        return plan

    def plan_expr(self, sql_expr: SQLExpr, input_schema: Schema) -> LogicalExpr:
        if isinstance(sql_expr, SQLColumnRef):
            if sql_expr.table is not None:
                col_name = f"{sql_expr.table}.{sql_expr.column}"
                try:
                    input_schema.field_index(col_name)
                    return Column(col_name)
                except ValueError:
                    return Column(sql_expr.column)
            return Column(sql_expr.column)

        if isinstance(sql_expr, SQLNumber):
            if "." in sql_expr.value:
                return LiteralDouble(float(sql_expr.value))
            return LiteralLong(int(sql_expr.value))

        if isinstance(sql_expr, SQLString):
            return LiteralString(sql_expr.value)

        if isinstance(sql_expr, SQLBooleanLiteral):
            return LiteralBoolean(sql_expr.value)

        if isinstance(sql_expr, SQLNull):
            return LiteralString("")

        if isinstance(sql_expr, SQLBinaryOp):
            left = self.plan_expr(sql_expr.left, input_schema)
            right = self.plan_expr(sql_expr.right, input_schema)
            return _make_binary(sql_expr.op, left, right)

        if isinstance(sql_expr, SQLUnaryOp):
            operand = self.plan_expr(sql_expr.operand, input_schema)
            if sql_expr.op == "NOT":
                return Not(operand)
            if sql_expr.op == "-":
                return Multiply(LiteralLong(-1), operand)
            raise ValueError(f"Unknown unary op: {sql_expr.op}")

        if isinstance(sql_expr, SQLFunction):
            if sql_expr.name in _AGG_FUNCS:
                if sql_expr.args and isinstance(sql_expr.args[0], SQLStar):
                    arg_expr = LiteralLong(1)
                else:
                    arg_expr = self.plan_expr(sql_expr.args[0], input_schema)
                return AggregateExpr(sql_expr.name, arg_expr)
            args = [self.plan_expr(a, input_schema) for a in sql_expr.args]
            from forge.logicalplan.expressions import ScalarFunction
            return ScalarFunction(sql_expr.name, args, DataType.Utf8)

        if isinstance(sql_expr, SQLCast):
            inner = self.plan_expr(sql_expr.expr, input_schema)
            dt = _TYPE_MAP.get(sql_expr.data_type)
            if dt is None:
                raise ValueError(f"Unknown data type: {sql_expr.data_type}")
            return Cast(inner, dt)

        if isinstance(sql_expr, SQLStar):
            raise ValueError("Star expression should be expanded before plan_expr")

        raise ValueError(f"Unknown SQL expression type: {type(sql_expr).__name__}")

    def _plan_projections(
        self, statement: SelectStatement, input_schema: Schema
    ) -> list[LogicalExpr]:
        exprs: list[LogicalExpr] = []
        for col in statement.columns:
            if isinstance(col.expr, SQLStar):
                for f in input_schema.fields:
                    exprs.append(Column(f.name))
            elif isinstance(col.expr, SQLColumnRef) and col.expr.column == "*":
                for f in input_schema.fields:
                    if col.expr.table and f.name.startswith(col.expr.table + "."):
                        exprs.append(Column(f.name))
                    elif col.expr.table is None:
                        exprs.append(Column(f.name))
            else:
                planned = self._plan_projection_expr(col.expr, input_schema)
                if col.alias:
                    planned = Alias(planned, col.alias)
                exprs.append(planned)
        return exprs

    def _plan_projection_expr(
        self, sql_expr: SQLExpr, input_schema: Schema
    ) -> LogicalExpr:
        if isinstance(sql_expr, SQLFunction) and sql_expr.name in _AGG_FUNCS:
            if sql_expr.args and isinstance(sql_expr.args[0], SQLStar):
                arg_expr = LiteralLong(1)
            else:
                arg_expr = self._plan_projection_expr(sql_expr.args[0], input_schema)
            agg = AggregateExpr(sql_expr.name, arg_expr)
            agg_name = str(agg)
            for f in input_schema.fields:
                if f.name == agg_name:
                    return Column(f.name)
            return self.plan_expr(sql_expr, input_schema)
        return self.plan_expr(sql_expr, input_schema)

    def _contains_aggregate(self, expr: SQLExpr) -> bool:
        if isinstance(expr, SQLFunction) and expr.name in _AGG_FUNCS:
            return True
        if isinstance(expr, SQLBinaryOp):
            return self._contains_aggregate(expr.left) or self._contains_aggregate(expr.right)
        if isinstance(expr, SQLUnaryOp):
            return self._contains_aggregate(expr.operand)
        if isinstance(expr, SQLCast):
            return self._contains_aggregate(expr.expr)
        return False

    def _collect_aggregates(
        self, statement: SelectStatement, input_schema: Schema
    ) -> list[AggregateExpr]:
        aggs: list[AggregateExpr] = []
        seen: set[str] = set()

        def _visit(expr: SQLExpr) -> None:
            if isinstance(expr, SQLFunction) and expr.name in _AGG_FUNCS:
                if expr.args and isinstance(expr.args[0], SQLStar):
                    arg_expr = LiteralLong(1)
                else:
                    arg_expr = self.plan_expr(expr.args[0], input_schema)
                agg = AggregateExpr(expr.name, arg_expr)
                key = str(agg)
                if key not in seen:
                    seen.add(key)
                    aggs.append(agg)
                return
            if isinstance(expr, SQLBinaryOp):
                _visit(expr.left)
                _visit(expr.right)
            elif isinstance(expr, SQLUnaryOp):
                _visit(expr.operand)
            elif isinstance(expr, SQLCast):
                _visit(expr.expr)

        for col in statement.columns:
            _visit(col.expr)
        if statement.having:
            _visit(statement.having)
        return aggs

    def _extract_join_on(
        self, expr: SQLExpr, combined_schema: Schema
    ) -> list[tuple[LogicalExpr, LogicalExpr]]:
        if isinstance(expr, SQLBinaryOp) and expr.op == "AND":
            left_pairs = self._extract_join_on(expr.left, combined_schema)
            right_pairs = self._extract_join_on(expr.right, combined_schema)
            return left_pairs + right_pairs
        if isinstance(expr, SQLBinaryOp) and expr.op == "=":
            left = self.plan_expr(expr.left, combined_schema)
            right = self.plan_expr(expr.right, combined_schema)
            return [(left, right)]
        raise ValueError("JOIN ON clause must contain equality conditions combined with AND")


def _make_binary(op: str, left: LogicalExpr, right: LogicalExpr) -> LogicalExpr:
    ops = {
        "=": Eq, "!=": Neq, "<": Lt, ">": Gt, "<=": LtEq, ">=": GtEq,
        "AND": And, "OR": Or,
        "+": Add, "-": Subtract, "*": Multiply, "/": Divide,
    }
    cls = ops.get(op)
    if cls is None:
        raise ValueError(f"Unknown binary operator: {op}")
    return cls(left, right)
