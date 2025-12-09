from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator

from forge.datatypes import RecordBatch


class PartitionStrategy(ABC):
    @abstractmethod
    def partition(
        self, batches: Iterator[RecordBatch], num_partitions: int,
    ) -> list[list[RecordBatch]]:
        ...


class RoundRobinPartition(PartitionStrategy):
    def partition(
        self, batches: Iterator[RecordBatch], num_partitions: int,
    ) -> list[list[RecordBatch]]:
        partitions: list[list[RecordBatch]] = [[] for _ in range(num_partitions)]
        for i, batch in enumerate(batches):
            partitions[i % num_partitions].append(batch)
        return partitions


class HashPartition(PartitionStrategy):
    def __init__(self, key_index: int) -> None:
        self._key_index = key_index

    def partition(
        self, batches: Iterator[RecordBatch], num_partitions: int,
    ) -> list[list[RecordBatch]]:
        import pyarrow as pa
        from forge.datatypes import ArrowVector, Field, Schema

        partitions: list[list[RecordBatch]] = [[] for _ in range(num_partitions)]
        for batch in batches:
            arrow_batch = batch.to_arrow()
            key_col = arrow_batch.column(self._key_index)
            bucket_indices: list[list[int]] = [[] for _ in range(num_partitions)]
            for row in range(arrow_batch.num_rows):
                h = hash(key_col[row].as_py()) % num_partitions
                bucket_indices[h].append(row)
            for p, indices in enumerate(bucket_indices):
                if indices:
                    sub = arrow_batch.take(pa.array(indices))
                    partitions[p].append(RecordBatch.from_arrow(sub))
        return partitions
