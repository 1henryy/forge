import pyarrow as pa
import pytest

from forge.datatypes import (
    DataType, Field, Schema, ArrowVector, RecordBatch,
)
from forge.parallel.partition import RoundRobinPartition, HashPartition


def _make_batch(ids, groups):
    schema = Schema([
        Field("id", DataType.Int64),
        Field("group", DataType.Utf8),
    ])
    columns = [
        ArrowVector(pa.array(ids, type=pa.int64())),
        ArrowVector(pa.array(groups, type=pa.utf8())),
    ]
    return RecordBatch(schema, columns)


class TestRoundRobinPartition:
    def test_distributes_evenly(self):
        batches = [_make_batch([i], [f"g{i}"]) for i in range(6)]
        rr = RoundRobinPartition()
        parts = rr.partition(iter(batches), 3)
        assert len(parts) == 3
        assert len(parts[0]) == 2
        assert len(parts[1]) == 2
        assert len(parts[2]) == 2

    def test_single_partition(self):
        batches = [_make_batch([1], ["a"]), _make_batch([2], ["b"])]
        rr = RoundRobinPartition()
        parts = rr.partition(iter(batches), 1)
        assert len(parts) == 1
        assert len(parts[0]) == 2

    def test_more_partitions_than_batches(self):
        batches = [_make_batch([1], ["a"])]
        rr = RoundRobinPartition()
        parts = rr.partition(iter(batches), 4)
        assert len(parts) == 4
        total = sum(len(p) for p in parts)
        assert total == 1

    def test_empty_input(self):
        rr = RoundRobinPartition()
        parts = rr.partition(iter([]), 3)
        assert len(parts) == 3
        assert all(len(p) == 0 for p in parts)

    def test_preserves_data(self):
        batches = [_make_batch([1, 2], ["a", "b"]), _make_batch([3, 4], ["c", "d"])]
        rr = RoundRobinPartition()
        parts = rr.partition(iter(batches), 2)
        all_ids = []
        for part in parts:
            for batch in part:
                for i in range(batch.row_count):
                    all_ids.append(batch.column(0).get_value(i))
        assert sorted(all_ids) == [1, 2, 3, 4]


class TestHashPartition:
    def test_same_key_same_partition(self):
        batch = _make_batch([1, 2, 3, 1, 2], ["a", "b", "c", "a", "b"])
        hp = HashPartition(key_index=1)
        parts = hp.partition(iter([batch]), 3)
        key_to_partition = {}
        for p_idx, part in enumerate(parts):
            for b in part:
                for i in range(b.row_count):
                    key = b.column(1).get_value(i)
                    if key in key_to_partition:
                        assert key_to_partition[key] == p_idx
                    else:
                        key_to_partition[key] = p_idx

    def test_all_data_preserved(self):
        batch = _make_batch([1, 2, 3, 4, 5], ["a", "b", "c", "d", "e"])
        hp = HashPartition(key_index=0)
        parts = hp.partition(iter([batch]), 3)
        all_ids = []
        for part in parts:
            for b in part:
                for i in range(b.row_count):
                    all_ids.append(b.column(0).get_value(i))
        assert sorted(all_ids) == [1, 2, 3, 4, 5]

    def test_single_partition_gets_all(self):
        batch = _make_batch([1, 2, 3], ["a", "b", "c"])
        hp = HashPartition(key_index=0)
        parts = hp.partition(iter([batch]), 1)
        total = sum(b.row_count for b in parts[0])
        assert total == 3

    def test_multiple_batches(self):
        b1 = _make_batch([1, 2], ["a", "b"])
        b2 = _make_batch([1, 3], ["a", "c"])
        hp = HashPartition(key_index=1)
        parts = hp.partition(iter([b1, b2]), 4)
        key_to_partition = {}
        for p_idx, part in enumerate(parts):
            for b in part:
                for i in range(b.row_count):
                    key = b.column(1).get_value(i)
                    if key in key_to_partition:
                        assert key_to_partition[key] == p_idx
                    else:
                        key_to_partition[key] = p_idx
        assert "a" in key_to_partition
