from abc import ABC, abstractmethod
from typing import Iterator

from forge.datatypes import Schema, RecordBatch


class PhysicalPlan(ABC):
    @abstractmethod
    def schema(self) -> Schema:
        ...

    @abstractmethod
    def execute(self) -> Iterator[RecordBatch]:
        ...

    @abstractmethod
    def children(self) -> list["PhysicalPlan"]:
        ...

    @abstractmethod
    def __str__(self) -> str:
        ...

    def format(self, indent: int = 0) -> str:
        result = "  " * indent + str(self) + "\n"
        for child in self.children():
            result += child.format(indent + 1)
        return result
