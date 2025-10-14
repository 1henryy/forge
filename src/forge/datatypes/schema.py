from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from .types import DataType


@dataclass(frozen=True)
class Field:
    name: str
    data_type: DataType


class Schema:
    def __init__(self, fields: list[Field]) -> None:
        self._fields = list(fields)

    @property
    def fields(self) -> list[Field]:
        return list(self._fields)

    def project(self, indices: list[int]) -> Schema:
        return Schema([self._fields[i] for i in indices])

    def field_index(self, name: str) -> int:
        for i, f in enumerate(self._fields):
            if f.name == name:
                return i
        raise ValueError(f"Field '{name}' not found in schema")

    def __len__(self) -> int:
        return len(self._fields)

    def __iter__(self) -> Iterator[Field]:
        return iter(self._fields)

    def __getitem__(self, index: int) -> Field:
        return self._fields[index]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schema):
            return NotImplemented
        return self._fields == other._fields

    def __str__(self) -> str:
        field_strs = [f"{f.name}: {f.data_type.name}" for f in self._fields]
        return f"Schema([{', '.join(field_strs)}])"
