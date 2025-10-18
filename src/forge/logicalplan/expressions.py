# __MARKER_1__
from __future__ import annotations

from abc import ABC, abstractmethod

from forge.datatypes import DataType, Field, Schema


class LogicalExpr(ABC):
    @abstractmethod
    def to_field(self, input_schema: Schema) -> Field:
        ...

    @abstractmethod
    def __str__(self) -> str:
        ...


class Column(LogicalExpr):
    def __init__(self, name: str) -> None:
        self.name = name

    def to_field(self, input_schema: Schema) -> Field:
        idx = input_schema.field_index(self.name)
        return input_schema[idx]

    def __str__(self) -> str:
        return f"#{self.name}"


class ColumnIndex(LogicalExpr):
    def __init__(self, index: int) -> None:
        self.index = index

    def to_field(self, input_schema: Schema) -> Field:
        return input_schema[self.index]

    def __str__(self) -> str:
        return f"#{self.index}"


class LiteralString(LogicalExpr):
    def __init__(self, value: str) -> None:
        self.value = value

    def to_field(self, input_schema: Schema) -> Field:
        return Field("lit", DataType.Utf8)

    def __str__(self) -> str:
        return f"'{self.value}'"


class LiteralLong(LogicalExpr):
    def __init__(self, value: int) -> None:
        self.value = value

    def to_field(self, input_schema: Schema) -> Field:
        return Field("lit", DataType.Int64)

    def __str__(self) -> str:
        return str(self.value)


class LiteralDouble(LogicalExpr):
    def __init__(self, value: float) -> None:
        self.value = value

    def to_field(self, input_schema: Schema) -> Field:
        return Field("lit", DataType.Float64)

    def __str__(self) -> str:
        return str(self.value)


class LiteralBoolean(LogicalExpr):
    def __init__(self, value: bool) -> None:
        self.value = value

    def to_field(self, input_schema: Schema) -> Field:
        return Field("lit", DataType.Boolean)

    def __str__(self) -> str:
        return str(self.value).upper()


_COMPARISON_OPS = {"=", "!=", "<", ">", "<=", ">="}
_BOOLEAN_OPS = {"AND", "OR"}


class BinaryExpr(LogicalExpr):
    def __init__(self, name: str, op: str, left: LogicalExpr, right: LogicalExpr) -> None:
        self.name = name
        self.op = op
        self.left = left
        self.right = right

    def to_field(self, input_schema: Schema) -> Field:
        if self.op in _COMPARISON_OPS or self.op in _BOOLEAN_OPS:
            return Field(self.name, DataType.Boolean)
        return Field(self.name, self.left.to_field(input_schema).data_type)

    def __str__(self) -> str:
        return f"{self.left} {self.op} {self.right}"


class Eq(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} = {right}", "=", left, right)


class Neq(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} != {right}", "!=", left, right)


class Lt(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} < {right}", "<", left, right)


class LtEq(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} <= {right}", "<=", left, right)


class Gt(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} > {right}", ">", left, right)


class GtEq(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} >= {right}", ">=", left, right)


class And(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} AND {right}", "AND", left, right)


class Or(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} OR {right}", "OR", left, right)


class Add(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} + {right}", "+", left, right)


class Subtract(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} - {right}", "-", left, right)


class Multiply(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} * {right}", "*", left, right)


class Divide(BinaryExpr):
    def __init__(self, left: LogicalExpr, right: LogicalExpr) -> None:
        super().__init__(f"{left} / {right}", "/", left, right)


class Not(LogicalExpr):
    def __init__(self, expr: LogicalExpr) -> None:
        self.expr = expr

    def to_field(self, input_schema: Schema) -> Field:
        return Field(f"NOT {self.expr}", DataType.Boolean)

    def __str__(self) -> str:
        return f"NOT {self.expr}"


class AggregateExpr(LogicalExpr):
    def __init__(self, name: str, expr: LogicalExpr) -> None:
        self.name = name
        self.expr = expr

    def to_field(self, input_schema: Schema) -> Field:
        if self.name == "COUNT":
            return Field(f"{self.name}({self.expr})", DataType.Int64)
        inner = self.expr.to_field(input_schema)
        return Field(f"{self.name}({self.expr})", inner.data_type)

    def __str__(self) -> str:
        return f"{self.name}({self.expr})"


class ScalarFunction(LogicalExpr):
    def __init__(self, name: str, args: list[LogicalExpr], return_type: DataType) -> None:
        self.name = name
        self.args = args
        self.return_type = return_type

    def to_field(self, input_schema: Schema) -> Field:
        return Field(self.name, self.return_type)

    def __str__(self) -> str:
        args_str = ", ".join(str(a) for a in self.args)
        return f"{self.name}({args_str})"


class Cast(LogicalExpr):
    def __init__(self, expr: LogicalExpr, data_type: DataType) -> None:
        self.expr = expr
        self.data_type = data_type

    def to_field(self, input_schema: Schema) -> Field:
        inner = self.expr.to_field(input_schema)
        return Field(inner.name, self.data_type)

    def __str__(self) -> str:
        return f"CAST({self.expr} AS {self.data_type.name})"


class Alias(LogicalExpr):
    def __init__(self, expr: LogicalExpr, alias: str) -> None:
        self.expr = expr
        self.alias = alias

    def to_field(self, input_schema: Schema) -> Field:
        inner = self.expr.to_field(input_schema)
        return Field(self.alias, inner.data_type)

    def __str__(self) -> str:
        return f"{self.expr} AS {self.alias}"


def col(name: str) -> Column:
    return Column(name)


def lit(value: str | int | float | bool) -> LogicalExpr:
    if isinstance(value, bool):
        return LiteralBoolean(value)
    if isinstance(value, int):
        return LiteralLong(value)
    if isinstance(value, float):
        return LiteralDouble(value)
    if isinstance(value, str):
        return LiteralString(value)
    raise ValueError(f"Unsupported literal type: {type(value)}")


def sum_expr(expr: LogicalExpr) -> AggregateExpr:
    return AggregateExpr("SUM", expr)


def min_expr(expr: LogicalExpr) -> AggregateExpr:
    return AggregateExpr("MIN", expr)


def max_expr(expr: LogicalExpr) -> AggregateExpr:
    return AggregateExpr("MAX", expr)


def avg_expr(expr: LogicalExpr) -> AggregateExpr:
    return AggregateExpr("AVG", expr)


def count_expr(expr: LogicalExpr) -> AggregateExpr:
    return AggregateExpr("COUNT", expr)
