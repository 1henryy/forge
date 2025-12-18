import pyarrow as pa
import pytest

from forge.datatypes import (
    DataType, Field, Schema, ArrowVector, LiteralVector, RecordBatch,
    to_arrow_type, from_arrow_type,
)


class TestDataType:
    def test_enum_values_exist(self):
        assert DataType.Int8 is not None
        assert DataType.Int16 is not None
        assert DataType.Int32 is not None
        assert DataType.Int64 is not None
        assert DataType.Float32 is not None
        assert DataType.Float64 is not None
        assert DataType.Utf8 is not None
        assert DataType.Boolean is not None

    @pytest.mark.parametrize("dt,arrow_type", [
        (DataType.Int8, pa.int8()),
        (DataType.Int16, pa.int16()),
        (DataType.Int32, pa.int32()),
        (DataType.Int64, pa.int64()),
        (DataType.Float32, pa.float32()),
        (DataType.Float64, pa.float64()),
        (DataType.Utf8, pa.utf8()),
        (DataType.Boolean, pa.bool_()),
    ])
    def test_to_arrow_type(self, dt, arrow_type):
        assert to_arrow_type(dt) == arrow_type

    @pytest.mark.parametrize("dt,arrow_type", [
        (DataType.Int8, pa.int8()),
        (DataType.Int16, pa.int16()),
        (DataType.Int32, pa.int32()),
        (DataType.Int64, pa.int64()),
        (DataType.Float32, pa.float32()),
        (DataType.Float64, pa.float64()),
        (DataType.Utf8, pa.utf8()),
        (DataType.Boolean, pa.bool_()),
    ])
    def test_from_arrow_type(self, dt, arrow_type):
        assert from_arrow_type(arrow_type) == dt

    @pytest.mark.parametrize("dt", list(DataType))
    def test_round_trip(self, dt):
        assert from_arrow_type(to_arrow_type(dt)) == dt

    def test_from_arrow_type_unsupported(self):
        with pytest.raises(ValueError, match="Unsupported Arrow type"):
            from_arrow_type(pa.binary())


class TestFieldAndSchema:
    def test_field_creation(self):
        f = Field("x", DataType.Int64)
        assert f.name == "x"
        assert f.data_type == DataType.Int64

    def test_field_equality(self):
        assert Field("x", DataType.Int64) == Field("x", DataType.Int64)
        assert Field("x", DataType.Int64) != Field("y", DataType.Int64)
        assert Field("x", DataType.Int64) != Field("x", DataType.Utf8)

    def test_schema_creation(self, simple_schema):
        assert len(simple_schema) == 3

    def test_schema_fields(self, simple_schema):
        fields = simple_schema.fields
        assert fields[0] == Field("id", DataType.Int64)
        assert fields[1] == Field("name", DataType.Utf8)
        assert fields[2] == Field("value", DataType.Float64)

    def test_schema_project(self, simple_schema):
        projected = simple_schema.project([0, 2])
        assert len(projected) == 2
        assert projected[0] == Field("id", DataType.Int64)
        assert projected[1] == Field("value", DataType.Float64)

    def test_schema_field_index(self, simple_schema):
        assert simple_schema.field_index("id") == 0
        assert simple_schema.field_index("name") == 1
        assert simple_schema.field_index("value") == 2

    def test_schema_field_index_not_found(self, simple_schema):
        with pytest.raises(ValueError, match="not found"):
            simple_schema.field_index("missing")

    def test_schema_equality(self):
        s1 = Schema([Field("a", DataType.Int64)])
        s2 = Schema([Field("a", DataType.Int64)])
        s3 = Schema([Field("b", DataType.Int64)])
        assert s1 == s2
        assert s1 != s3

    def test_schema_iteration(self, simple_schema):
        names = [f.name for f in simple_schema]
        assert names == ["id", "name", "value"]

    def test_schema_getitem(self, simple_schema):
        assert simple_schema[1] == Field("name", DataType.Utf8)

    def test_schema_str(self, simple_schema):
        s = str(simple_schema)
        assert "id" in s
        assert "Int64" in s


class TestArrowVector:
    def test_creation_and_get_value(self):
        arr = pa.array([10, 20, 30], type=pa.int64())
        vec = ArrowVector(arr)
        assert vec.get_value(0) == 10
        assert vec.get_value(1) == 20
        assert vec.get_value(2) == 30

    def test_size(self):
        arr = pa.array([1, 2, 3, 4, 5], type=pa.int64())
        vec = ArrowVector(arr)
        assert vec.size == 5
        assert len(vec) == 5

    def test_dtype(self):
        vec = ArrowVector(pa.array([1, 2], type=pa.int64()))
        assert vec.dtype == DataType.Int64

    def test_dtype_utf8(self):
        vec = ArrowVector(pa.array(["hello"], type=pa.utf8()))
        assert vec.dtype == DataType.Utf8

    def test_to_pyarrow(self):
        arr = pa.array([1.0, 2.0], type=pa.float64())
        vec = ArrowVector(arr)
        assert vec.to_pyarrow() == arr


class TestLiteralVector:
    def test_creation(self):
        vec = LiteralVector(42, DataType.Int64, 5)
        assert vec.size == 5
        assert vec.dtype == DataType.Int64

    def test_get_value_always_returns_same(self):
        vec = LiteralVector("hello", DataType.Utf8, 10)
        for i in range(10):
            assert vec.get_value(i) == "hello"

    def test_len(self):
        vec = LiteralVector(True, DataType.Boolean, 3)
        assert len(vec) == 3


class TestRecordBatch:
    def test_creation(self, simple_batch, simple_schema):
        assert simple_batch.schema == simple_schema

    def test_row_count(self, simple_batch):
        assert simple_batch.row_count == 3

    def test_column_count(self, simple_batch):
        assert simple_batch.column_count == 3

    def test_column_access(self, simple_batch):
        col = simple_batch.column(0)
        assert col.get_value(0) == 1

    def test_field_access(self, simple_batch):
        f = simple_batch.field(1)
        assert f.name == "name"

    def test_empty_batch_row_count(self):
        schema = Schema([Field("x", DataType.Int64)])
        batch = RecordBatch(schema, [])
        assert batch.row_count == 0

    def test_to_arrow(self, simple_batch):
        arrow_batch = simple_batch.to_arrow()
        assert isinstance(arrow_batch, pa.RecordBatch)
        assert arrow_batch.num_rows == 3
        assert arrow_batch.num_columns == 3

    def test_from_arrow(self):
        arrow_batch = pa.RecordBatch.from_pydict({
            "a": [1, 2],
            "b": ["x", "y"],
        })
        batch = RecordBatch.from_arrow(arrow_batch)
        assert batch.row_count == 2
        assert batch.column_count == 2
        assert batch.column(0).get_value(0) == 1
        assert batch.column(1).get_value(1) == "y"

    def test_round_trip(self, simple_batch):
        arrow_batch = simple_batch.to_arrow()
        roundtripped = RecordBatch.from_arrow(arrow_batch)
        assert roundtripped.row_count == simple_batch.row_count
        assert roundtripped.column_count == simple_batch.column_count
        for i in range(simple_batch.row_count):
            for j in range(simple_batch.column_count):
                assert roundtripped.column(j).get_value(i) == simple_batch.column(j).get_value(i)

    def test_to_arrow_with_literal_vector(self):
        schema = Schema([Field("x", DataType.Int64)])
        col = LiteralVector(42, DataType.Int64, 3)
        batch = RecordBatch(schema, [col])
        arrow_batch = batch.to_arrow()
        assert arrow_batch.num_rows == 3
        assert arrow_batch.column(0).to_pylist() == [42, 42, 42]
