from abc import ABC, abstractmethod
from typing import Any

from .types import DataType


class ColumnVector(ABC):
    @property
    @abstractmethod
    def dtype(self) -> DataType: ...

    @property
    @abstractmethod
    def size(self) -> int: ...

    @abstractmethod
    def get_value(self, index: int) -> Any: ...

    def __len__(self) -> int:
        return self.size
