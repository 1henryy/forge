from abc import ABC, abstractmethod
from typing import Iterator

from forge.datatypes import Schema, RecordBatch


class DataSource(ABC):
    @abstractmethod
    def schema(self) -> Schema:
        ...

    @abstractmethod
    def scan(self, projection: list[int] | None = None) -> Iterator[RecordBatch]:
        ...
