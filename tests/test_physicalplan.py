import pyarrow as pa
import pytest

from forge.datatypes import (
    DataType, Field, Schema, ArrowVector, LiteralVector, RecordBatch,
)
from forge.datasources.memory_source import MemorySource
from forge.physicalplan.expressions import (
    ColumnExpr, LiteralLongExpr, LiteralDoubleExpr, LiteralStringExpr,
    LiteralBoolExpr, BinaryExpr, SumExpr, CountExpr, MinExpr, MaxExpr, AvgExpr,
)
from forge.physicalplan.scan_exec import ScanExec
from forge.physicalplan.selection_exec import SelectionExec
from forge.physicalplan.projection_exec import ProjectionExec
from forge.physicalplan.hash_aggregate_exec import HashAggregateExec
from forge.physicalplan.sort_exec import SortExec
from forge.physicalplan.limit_exec import LimitExec


@pytest.fixture
def sample_batch():
    schema = Schema([
        Field("id", DataType.Int64),
        Field("group", DataType.Utf8),
        Field("value", DataType.Int64),
    ])
    columns = [
        ArrowVector(pa.array([1, 2, 3, 4], type=pa.int64())),
        ArrowVector(pa.array(["a", "b", "a", "b"], type=pa.utf8())),
        ArrowVector(pa.array([10, 20, 30, 40], type=pa.int64())),
    ]
    return RecordBatch(schema, columns)


@pytest.fixture
def sample_source(sample_batch):
    return MemorySource(sample_batch.schema, [sample_batch])


class TestPhysicalExpressions:
    def test_column_expr(self, sample_batch):
        expr = ColumnExpr(0)
        result = expr.evaluate(sample_batch)
        assert result.get_value(0) == 1
        assert result.get_value(3) == 4

    def test_literal_long_expr(self, sample_batch):
        expr = LiteralLongExpr(42)
        result = expr.evaluate(sample_batch)
        assert result.size == 4
        assert result.get_value(0) == 42
        assert result.get_value(3) == 42

    def test_literal_double_expr(self, sample_batch):
        expr = LiteralDoubleExpr(3.14)
        result = expr.evaluate(sample_batch)
        assert result.get_value(0) == 3.14

    def test_literal_string_expr(self, sample_batch):
        expr = LiteralStringExpr("hello")
        result = expr.evaluate(sample_batch)
        assert result.get_value(0) == "hello"

    def test_literal_bool_expr(self, sample_batch):
        expr = LiteralBoolExpr(True)
        result = expr.evaluate(sample_batch)
        assert result.get_value(0) is True

    def test_binary_add(self, sample_batch):
        expr = BinaryExpr(ColumnExpr(0), "add", LiteralLongExpr(100))
        result = expr.evaluate(sample_batch)
        assert result.get_value(0) == 101
        assert result.get_value(1) == 102

    def test_binary_equal(self, sample_batch):
        expr = BinaryExpr(ColumnExpr(0), "equal", LiteralLongExpr(2))
        result = expr.evaluate(sample_batch)
        assert result.get_value(0) is False
        assert result.get_value(1) is True
        assert result.get_value(2) is False

    def test_binary_greater(self, sample_batch):
        expr = BinaryExpr(ColumnExpr(2), "greater", LiteralLongExpr(15))
        result = expr.evaluate(sample_batch)
        assert result.get_value(0) is False
        assert result.get_value(1) is True

    def test_binary_multiply(self, sample_batch):
        expr = BinaryExpr(ColumnExpr(2), "multiply", LiteralLongExpr(2))
        result = expr.evaluate(sample_batch)
        assert result.get_value(0) == 20
        assert result.get_value(2) == 60


class TestSelectionExec:
    def test_filter_rows(self, sample_source):
        scan = ScanExec(sample_source)
        filter_expr = BinaryExpr(ColumnExpr(2), "greater", LiteralLongExpr(15))
        sel = SelectionExec(scan, filter_expr)
        batches = list(sel.execute())
        total_rows = sum(b.row_count for b in batches)
        assert total_rows == 3  # values 20, 30, 40

    def test_filter_all(self, sample_source):
        scan = ScanExec(sample_source)
        filter_expr = BinaryExpr(ColumnExpr(0), "greater", LiteralLongExpr(100))
        sel = SelectionExec(scan, filter_expr)
        batches = list(sel.execute())
        assert len(batches) == 0

    def test_filter_none(self, sample_source):
        scan = ScanExec(sample_source)
        filter_expr = BinaryExpr(ColumnExpr(0), "greater", LiteralLongExpr(0))
        sel = SelectionExec(scan, filter_expr)
        batches = list(sel.execute())
        total_rows = sum(b.row_count for b in batches)
        assert total_rows == 4


class TestHashAggregateExec:
    def test_group_by_sum(self, sample_source):
        scan = ScanExec(sample_source)
        out_schema = Schema([
            Field("group", DataType.Utf8),
            Field("SUM(value)", DataType.Int64),
        ])
        agg = HashAggregateExec(
            scan,
            group_exprs=[ColumnExpr(1)],
            agg_exprs=[SumExpr(ColumnExpr(2))],
            schema=out_schema,
        )
        batches = list(agg.execute())
        assert len(batches) == 1
        batch = batches[0]
        assert batch.row_count == 2
        results = {}
        for i in range(batch.row_count):
            key = batch.column(0).get_value(i)
            val = batch.column(1).get_value(i)
            results[key] = val
        assert results["a"] == 40  # 10 + 30
        assert results["b"] == 60  # 20 + 40

    def test_count(self, sample_source):
        scan = ScanExec(sample_source)
        out_schema = Schema([
            Field("group", DataType.Utf8),
            Field("COUNT(id)", DataType.Int64),
        ])
        agg = HashAggregateExec(
            scan,
            group_exprs=[ColumnExpr(1)],
            agg_exprs=[CountExpr(ColumnExpr(0))],
            schema=out_schema,
        )
        batches = list(agg.execute())
        batch = batches[0]
        results = {}
        for i in range(batch.row_count):
            results[batch.column(0).get_value(i)] = batch.column(1).get_value(i)
        assert results["a"] == 2
        assert results["b"] == 2

    def test_min_max(self, sample_source):
        scan = ScanExec(sample_source)
        out_schema = Schema([
            Field("group", DataType.Utf8),
            Field("MIN(value)", DataType.Int64),
            Field("MAX(value)", DataType.Int64),
        ])
        agg = HashAggregateExec(
            scan,
            group_exprs=[ColumnExpr(1)],
            agg_exprs=[MinExpr(ColumnExpr(2)), MaxExpr(ColumnExpr(2))],
            schema=out_schema,
        )
        batches = list(agg.execute())
        batch = batches[0]
        results = {}
        for i in range(batch.row_count):
            key = batch.column(0).get_value(i)
            results[key] = (batch.column(1).get_value(i), batch.column(2).get_value(i))
        assert results["a"] == (10, 30)
        assert results["b"] == (20, 40)

    def test_avg(self, sample_source):
        scan = ScanExec(sample_source)
        out_schema = Schema([
            Field("group", DataType.Utf8),
            Field("AVG(value)", DataType.Float64),
        ])
        agg = HashAggregateExec(
            scan,
            group_exprs=[ColumnExpr(1)],
            agg_exprs=[AvgExpr(ColumnExpr(2))],
            schema=out_schema,
        )
        batches = list(agg.execute())
        batch = batches[0]
        results = {}
        for i in range(batch.row_count):
            results[batch.column(0).get_value(i)] = batch.column(1).get_value(i)
        assert results["a"] == pytest.approx(20.0)
        assert results["b"] == pytest.approx(30.0)


class TestSortExec:
    def test_sort_ascending(self, sample_source):
        scan = ScanExec(sample_source)
        sort = SortExec(scan, [(ColumnExpr(2), True)])
        batches = list(sort.execute())
        values = []
        for b in batches:
            for i in range(b.row_count):
                values.append(b.column(2).get_value(i))
        assert values == [10, 20, 30, 40]

    def test_sort_descending(self, sample_source):
        scan = ScanExec(sample_source)
        sort = SortExec(scan, [(ColumnExpr(2), False)])
        batches = list(sort.execute())
        values = []
        for b in batches:
            for i in range(b.row_count):
                values.append(b.column(2).get_value(i))
        assert values == [40, 30, 20, 10]

    def test_sort_preserves_schema(self, sample_source):
        scan = ScanExec(sample_source)
        sort = SortExec(scan, [(ColumnExpr(0), True)])
        assert sort.schema() == scan.schema()


class TestLimitExec:
    def test_limit_rows(self, sample_source):
        scan = ScanExec(sample_source)
        limit = LimitExec(scan, 2)
        batches = list(limit.execute())
        total = sum(b.row_count for b in batches)
        assert total == 2

    def test_limit_larger_than_data(self, sample_source):
        scan = ScanExec(sample_source)
        limit = LimitExec(scan, 100)
        batches = list(limit.execute())
        total = sum(b.row_count for b in batches)
        assert total == 4

    def test_limit_zero(self, sample_source):
        scan = ScanExec(sample_source)
        limit = LimitExec(scan, 0)
        batches = list(limit.execute())
        total = sum(b.row_count for b in batches)
        assert total == 0

    def test_limit_preserves_schema(self, sample_source):
        scan = ScanExec(sample_source)
        limit = LimitExec(scan, 2)
        assert limit.schema() == scan.schema()
