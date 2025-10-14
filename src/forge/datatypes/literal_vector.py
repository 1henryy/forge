from typing import Any

from .column_vector import ColumnVector
from .types import DataType


class LiteralVector(ColumnVector):
    def __init__(self, value: Any, data_type: DataType, count: int) -> None:
        self._value = value
        self._data_type = data_type
        self._count = count

    @property
    def dtype(self) -> DataType:
        return self._data_type

    @property
    def size(self) -> int:
        return self._count

    def get_value(self, index: int) -> Any:
        return self._value
