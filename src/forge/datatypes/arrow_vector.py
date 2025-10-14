from typing import Any

import pyarrow as pa

from .column_vector import ColumnVector
from .types import DataType, from_arrow_type


class ArrowVector(ColumnVector):
    def __init__(self, array: pa.Array) -> None:
        self._array = array

    @property
    def dtype(self) -> DataType:
        return from_arrow_type(self._array.type)

    @property
    def size(self) -> int:
        return len(self._array)

    def get_value(self, index: int) -> Any:
        return self._array[index].as_py()

    def to_pyarrow(self) -> pa.Array:
        return self._array
