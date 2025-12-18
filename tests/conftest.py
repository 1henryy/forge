import pytest
import pyarrow as pa

from forge.datatypes import (
    DataType, Field, Schema, ArrowVector, LiteralVector, RecordBatch,
)
from forge.datasources.memory_source import MemorySource
from forge.execution.context import ExecutionContext


@pytest.fixture
def simple_schema():
    return Schema([
        Field("id", DataType.Int64),
        Field("name", DataType.Utf8),
        Field("value", DataType.Float64),
    ])


@pytest.fixture
def simple_batch(simple_schema):
    columns = [
        ArrowVector(pa.array([1, 2, 3], type=pa.int64())),
        ArrowVector(pa.array(["a", "b", "c"], type=pa.utf8())),
        ArrowVector(pa.array([1.0, 2.0, 3.0], type=pa.float64())),
    ]
    return RecordBatch(simple_schema, columns)


@pytest.fixture
def simple_source(simple_batch, simple_schema):
    return MemorySource(simple_schema, [simple_batch])


@pytest.fixture
def ctx():
    ctx = ExecutionContext()
    ctx.register_memory("t", {
        "id": [1, 2, 3, 4, 5],
        "name": ["alice", "bob", "alice", "bob", "carol"],
        "value": [10, 20, 30, 40, 50],
    })
    return ctx
