from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from forge.datatypes import (
    ColumnVector, RecordBatch, ArrowVector, LiteralVector, DataType, to_arrow_type,
)


class PhysicalExpr(ABC):
    @abstractmethod
    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        ...


class ColumnExpr(PhysicalExpr):
    def __init__(self, index: int) -> None:
        self.index: int = index

    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        return batch.column(self.index)

    def __str__(self) -> str:
        return f"#{self.index}"


class LiteralLongExpr(PhysicalExpr):
    def __init__(self, value: int) -> None:
        self.value: int = value

    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        return LiteralVector(self.value, DataType.Int64, batch.row_count)

    def __str__(self) -> str:
        return str(self.value)


class LiteralDoubleExpr(PhysicalExpr):
    def __init__(self, value: float) -> None:
        self.value: float = value

    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        return LiteralVector(self.value, DataType.Float64, batch.row_count)

    def __str__(self) -> str:
        return str(self.value)


class LiteralStringExpr(PhysicalExpr):
    def __init__(self, value: str) -> None:
        self.value: str = value

    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        return LiteralVector(self.value, DataType.Utf8, batch.row_count)

    def __str__(self) -> str:
        return f"'{self.value}'"


class LiteralBoolExpr(PhysicalExpr):
    def __init__(self, value: bool) -> None:
        self.value: bool = value

    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        return LiteralVector(self.value, DataType.Boolean, batch.row_count)

    def __str__(self) -> str:
        return str(self.value)


def _to_arrow_array(col: ColumnVector) -> pa.Array:
    if isinstance(col, ArrowVector):
        return col.to_pyarrow()
    values = [col.get_value(i) for i in range(col.size)]
    return pa.array(values, type=to_arrow_type(col.dtype))


_BINARY_OPS: dict[str, Any] = {
    "add": pc.add,
    "subtract": pc.subtract,
    "multiply": pc.multiply,
    "divide": pc.divide,
    "equal": pc.equal,
    "not_equal": pc.not_equal,
    "less": pc.less,
    "greater": pc.greater,
    "less_equal": pc.less_equal,
    "greater_equal": pc.greater_equal,
    "and": pc.and_,
    "or": pc.or_,
}


class BinaryExpr(PhysicalExpr):
    def __init__(self, left: PhysicalExpr, op: str, right: PhysicalExpr) -> None:
        self.left: PhysicalExpr = left
        self.op: str = op
        self.right: PhysicalExpr = right

    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        left_val = self.left.evaluate(batch)
        right_val = self.right.evaluate(batch)
        left_arr = _to_arrow_array(left_val)
        right_arr = _to_arrow_array(right_val)
        func = _BINARY_OPS[self.op]
        result = func(left_arr, right_arr)
        return ArrowVector(result)

    def __str__(self) -> str:
        return f"{self.left} {self.op} {self.right}"


class NotExpr(PhysicalExpr):
    def __init__(self, expr: PhysicalExpr) -> None:
        self.expr: PhysicalExpr = expr

    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        val = self.expr.evaluate(batch)
        arr = _to_arrow_array(val)
        return ArrowVector(pc.invert(arr))

    def __str__(self) -> str:
        return f"NOT {self.expr}"


class CastExpr(PhysicalExpr):
    def __init__(self, expr: PhysicalExpr, data_type: DataType) -> None:
        self.expr: PhysicalExpr = expr
        self.data_type: DataType = data_type

    def evaluate(self, batch: RecordBatch) -> ColumnVector:
        val = self.expr.evaluate(batch)
        arr = _to_arrow_array(val)
        result = pc.cast(arr, to_arrow_type(self.data_type))
        return ArrowVector(result)

    def __str__(self) -> str:
        return f"CAST({self.expr} AS {self.data_type.name})"


class Accumulator(ABC):
    @abstractmethod
    def accumulate(self, values: ColumnVector) -> None:
        ...

    @abstractmethod
    def final_value(self) -> Any:
        ...


class AggregateExpr(ABC):
    @abstractmethod
    def input_expr(self) -> PhysicalExpr:
        ...

    @abstractmethod
    def create_accumulator(self) -> Accumulator:
        ...


class SumAccumulator(Accumulator):
    def __init__(self) -> None:
        self._sum: Any = None

    def accumulate(self, values: ColumnVector) -> None:
        for i in range(values.size):
            v = values.get_value(i)
            if v is not None:
                self._sum = v if self._sum is None else self._sum + v

    def final_value(self) -> Any:
        return self._sum


class CountAccumulator(Accumulator):
    def __init__(self) -> None:
        self._count: int = 0

    def accumulate(self, values: ColumnVector) -> None:
        for i in range(values.size):
            if values.get_value(i) is not None:
                self._count += 1

    def final_value(self) -> Any:
        return self._count


class MinAccumulator(Accumulator):
    def __init__(self) -> None:
        self._min: Any = None

    def accumulate(self, values: ColumnVector) -> None:
        for i in range(values.size):
            v = values.get_value(i)
            if v is not None:
                if self._min is None or v < self._min:
                    self._min = v

    def final_value(self) -> Any:
        return self._min


class MaxAccumulator(Accumulator):
    def __init__(self) -> None:
        self._max: Any = None

    def accumulate(self, values: ColumnVector) -> None:
        for i in range(values.size):
            v = values.get_value(i)
            if v is not None:
                if self._max is None or v > self._max:
                    self._max = v

    def final_value(self) -> Any:
        return self._max


class AvgAccumulator(Accumulator):
    def __init__(self) -> None:
        self._sum: float = 0.0
        self._count: int = 0

    def accumulate(self, values: ColumnVector) -> None:
        for i in range(values.size):
            v = values.get_value(i)
            if v is not None:
                self._sum += v
                self._count += 1

    def final_value(self) -> Any:
        if self._count == 0:
            return None
        return self._sum / self._count


class SumExpr(AggregateExpr):
    def __init__(self, expr: PhysicalExpr) -> None:
        self._expr: PhysicalExpr = expr

    def input_expr(self) -> PhysicalExpr:
        return self._expr

    def create_accumulator(self) -> Accumulator:
        return SumAccumulator()

    def __str__(self) -> str:
        return f"SUM({self._expr})"


class CountExpr(AggregateExpr):
    def __init__(self, expr: PhysicalExpr) -> None:
        self._expr: PhysicalExpr = expr

    def input_expr(self) -> PhysicalExpr:
        return self._expr

    def create_accumulator(self) -> Accumulator:
        return CountAccumulator()

    def __str__(self) -> str:
        return f"COUNT({self._expr})"


class MinExpr(AggregateExpr):
    def __init__(self, expr: PhysicalExpr) -> None:
        self._expr: PhysicalExpr = expr

    def input_expr(self) -> PhysicalExpr:
        return self._expr

    def create_accumulator(self) -> Accumulator:
        return MinAccumulator()

    def __str__(self) -> str:
        return f"MIN({self._expr})"


class MaxExpr(AggregateExpr):
    def __init__(self, expr: PhysicalExpr) -> None:
        self._expr: PhysicalExpr = expr

    def input_expr(self) -> PhysicalExpr:
        return self._expr

    def create_accumulator(self) -> Accumulator:
        return MaxAccumulator()

    def __str__(self) -> str:
        return f"MAX({self._expr})"


class AvgExpr(AggregateExpr):
    def __init__(self, expr: PhysicalExpr) -> None:
        self._expr: PhysicalExpr = expr

    def input_expr(self) -> PhysicalExpr:
        return self._expr

    def create_accumulator(self) -> Accumulator:
        return AvgAccumulator()

    def __str__(self) -> str:
        return f"AVG({self._expr})"
