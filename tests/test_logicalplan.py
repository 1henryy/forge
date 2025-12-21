import pytest

from forge.datatypes import DataType, Field, Schema
from forge.datasources.memory_source import MemorySource
from forge.logicalplan.expressions import (
    Column, ColumnIndex, LiteralLong, LiteralDouble, LiteralString,
    LiteralBoolean, BinaryExpr, Eq, Neq, Lt, Gt, Add, Subtract, Multiply,
    And, Or, Not, AggregateExpr, Cast, Alias, col, lit, sum_expr, count_expr,
)
from forge.logicalplan.plan import (
    Scan, Projection, Selection, Aggregate, Join, Sort, Limit,
)
from forge.logicalplan.dataframe import DataFrame


@pytest.fixture
def test_schema():
    return Schema([
        Field("id", DataType.Int64),
        Field("name", DataType.Utf8),
        Field("score", DataType.Float64),
    ])


@pytest.fixture
def test_source(test_schema):
    return MemorySource.from_pydict({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "score": [1.0, 2.0, 3.0],
    })


class TestExpressions:
    def test_column_to_field(self, test_schema):
        expr = Column("name")
        f = expr.to_field(test_schema)
        assert f.name == "name"
        assert f.data_type == DataType.Utf8

    def test_column_index_to_field(self, test_schema):
        expr = ColumnIndex(2)
        f = expr.to_field(test_schema)
        assert f.name == "score"
        assert f.data_type == DataType.Float64

    def test_literal_long_to_field(self, test_schema):
        expr = LiteralLong(42)
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Int64

    def test_literal_double_to_field(self, test_schema):
        expr = LiteralDouble(3.14)
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Float64

    def test_literal_string_to_field(self, test_schema):
        expr = LiteralString("hello")
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Utf8

    def test_literal_boolean_to_field(self, test_schema):
        expr = LiteralBoolean(True)
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Boolean

    def test_binary_comparison_returns_boolean(self, test_schema):
        expr = Eq(Column("id"), LiteralLong(1))
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Boolean

    def test_binary_neq_returns_boolean(self, test_schema):
        expr = Neq(Column("id"), LiteralLong(1))
        assert expr.to_field(test_schema).data_type == DataType.Boolean

    def test_binary_lt_returns_boolean(self, test_schema):
        expr = Lt(Column("id"), LiteralLong(1))
        assert expr.to_field(test_schema).data_type == DataType.Boolean

    def test_binary_gt_returns_boolean(self, test_schema):
        expr = Gt(Column("id"), LiteralLong(1))
        assert expr.to_field(test_schema).data_type == DataType.Boolean

    def test_binary_and_returns_boolean(self, test_schema):
        expr = And(LiteralBoolean(True), LiteralBoolean(False))
        assert expr.to_field(test_schema).data_type == DataType.Boolean

    def test_binary_or_returns_boolean(self, test_schema):
        expr = Or(LiteralBoolean(True), LiteralBoolean(False))
        assert expr.to_field(test_schema).data_type == DataType.Boolean

    def test_binary_math_returns_left_type(self, test_schema):
        expr = Add(Column("id"), LiteralLong(1))
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Int64

    def test_multiply_returns_left_type(self, test_schema):
        expr = Multiply(Column("score"), LiteralDouble(2.0))
        assert expr.to_field(test_schema).data_type == DataType.Float64

    def test_aggregate_count_returns_int64(self, test_schema):
        expr = AggregateExpr("COUNT", Column("id"))
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Int64

    def test_aggregate_sum_returns_inner_type(self, test_schema):
        expr = AggregateExpr("SUM", Column("score"))
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Float64

    def test_not_returns_boolean(self, test_schema):
        expr = Not(LiteralBoolean(True))
        assert expr.to_field(test_schema).data_type == DataType.Boolean

    def test_cast_changes_type(self, test_schema):
        expr = Cast(Column("id"), DataType.Float64)
        f = expr.to_field(test_schema)
        assert f.data_type == DataType.Float64

    def test_alias_changes_name(self, test_schema):
        expr = Alias(Column("id"), "my_id")
        f = expr.to_field(test_schema)
        assert f.name == "my_id"
        assert f.data_type == DataType.Int64

    def test_col_helper(self, test_schema):
        expr = col("name")
        assert isinstance(expr, Column)
        assert expr.name == "name"

    def test_lit_int(self, test_schema):
        expr = lit(42)
        assert isinstance(expr, LiteralLong)

    def test_lit_float(self, test_schema):
        expr = lit(3.14)
        assert isinstance(expr, LiteralDouble)

    def test_lit_str(self, test_schema):
        expr = lit("hello")
        assert isinstance(expr, LiteralString)

    def test_lit_bool(self, test_schema):
        expr = lit(True)
        assert isinstance(expr, LiteralBoolean)

    def test_expression_str(self, test_schema):
        expr = Add(Column("x"), LiteralLong(1))
        assert "+" in str(expr)


class TestPlanNodes:
    def test_scan_schema(self, test_source):
        plan = Scan(test_source, "test")
        schema = plan.schema()
        assert len(schema) == 3
        assert schema[0].name == "id"

    def test_scan_with_projection(self, test_source):
        plan = Scan(test_source, "test", projection=[0, 2])
        schema = plan.schema()
        assert len(schema) == 2
        assert schema[0].name == "id"
        assert schema[1].name == "score"

    def test_scan_children(self, test_source):
        plan = Scan(test_source, "test")
        assert plan.children() == []

    def test_projection_schema(self, test_source):
        scan = Scan(test_source, "test")
        proj = Projection(scan, [Column("id"), Column("name")])
        schema = proj.schema()
        assert len(schema) == 2
        assert schema[0].name == "id"
        assert schema[1].name == "name"

    def test_selection_schema(self, test_source):
        scan = Scan(test_source, "test")
        sel = Selection(scan, Gt(Column("id"), LiteralLong(1)))
        assert sel.schema() == scan.schema()

    def test_aggregate_schema(self, test_source):
        scan = Scan(test_source, "test")
        agg = Aggregate(
            scan,
            [Column("name")],
            [AggregateExpr("COUNT", Column("id"))],
        )
        schema = agg.schema()
        assert len(schema) == 2
        assert schema[0].name == "name"
        assert "COUNT" in schema[1].name

    def test_join_schema(self, test_source):
        left = Scan(test_source, "left")
        right_src = MemorySource.from_pydict({"id": [1], "extra": ["x"]})
        right = Scan(right_src, "right")
        join = Join(left, right, "INNER", [(Column("id"), Column("id"))])
        schema = join.schema()
        assert len(schema) == 5  # 3 from left + 2 from right

    def test_sort_schema(self, test_source):
        scan = Scan(test_source, "test")
        sort = Sort(scan, [(Column("id"), True)])
        assert sort.schema() == scan.schema()

    def test_limit_schema(self, test_source):
        scan = Scan(test_source, "test")
        limit = Limit(scan, 5)
        assert limit.schema() == scan.schema()

    def test_plan_format(self, test_source):
        scan = Scan(test_source, "test")
        proj = Projection(scan, [Column("id")])
        text = proj.format()
        assert "Projection" in text
        assert "Scan" in text

    def test_plan_children(self, test_source):
        scan = Scan(test_source, "test")
        proj = Projection(scan, [Column("id")])
        children = proj.children()
        assert len(children) == 1
        assert children[0] is scan


class TestDataFrame:
    def test_project(self, test_source):
        scan = Scan(test_source, "test")
        df = DataFrame(scan)
        df2 = df.project([Column("id")])
        schema = df2.schema()
        assert len(schema) == 1
        assert schema[0].name == "id"

    def test_filter(self, test_source):
        scan = Scan(test_source, "test")
        df = DataFrame(scan).filter(Gt(Column("id"), LiteralLong(1)))
        plan = df.logical_plan()
        assert isinstance(plan, Selection)

    def test_aggregate(self, test_source):
        scan = Scan(test_source, "test")
        df = DataFrame(scan).aggregate(
            [Column("name")],
            [AggregateExpr("SUM", Column("score"))],
        )
        plan = df.logical_plan()
        assert isinstance(plan, Aggregate)

    def test_sort(self, test_source):
        scan = Scan(test_source, "test")
        df = DataFrame(scan).sort([(Column("id"), False)])
        plan = df.logical_plan()
        assert isinstance(plan, Sort)

    def test_limit(self, test_source):
        scan = Scan(test_source, "test")
        df = DataFrame(scan).limit(5)
        plan = df.logical_plan()
        assert isinstance(plan, Limit)
        assert plan.limit == 5

    def test_chained(self, test_source):
        scan = Scan(test_source, "test")
        df = (
            DataFrame(scan)
            .filter(Gt(Column("id"), LiteralLong(1)))
            .project([Column("id"), Column("name")])
            .limit(10)
        )
        plan = df.logical_plan()
        assert isinstance(plan, Limit)
        assert isinstance(plan.input, Projection)
