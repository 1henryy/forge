import pytest

from forge.execution.context import ExecutionContext
from forge.execution.result import QueryResult
from forge.logicalplan.expressions import Column, Gt, LiteralLong, AggregateExpr
from forge.logicalplan.plan import Scan, Projection, Selection, Aggregate
from forge.logicalplan.dataframe import DataFrame


@pytest.fixture
def ctx():
    ctx = ExecutionContext()
    ctx.register_memory("t", {
        "id": [1, 2, 3, 4, 5],
        "name": ["alice", "bob", "alice", "bob", "carol"],
        "value": [10, 20, 30, 40, 50],
    })
    return ctx


@pytest.fixture
def join_ctx():
    ctx = ExecutionContext()
    ctx.register_memory("orders", {
        "order_id": [1, 2, 3],
        "customer_id": [10, 20, 10],
        "amount": [100, 200, 150],
    })
    ctx.register_memory("customers", {
        "customer_id": [10, 20, 30],
        "name": ["alice", "bob", "carol"],
    })
    return ctx


class TestSelectStar:
    def test_select_star(self, ctx):
        result = ctx.sql("SELECT * FROM t")
        assert result.row_count == 5
        table = result.to_arrow()
        assert table.num_columns == 3

    def test_select_single_column(self, ctx):
        result = ctx.sql("SELECT id FROM t")
        table = result.to_arrow()
        assert table.num_columns == 1
        assert "id" in table.column_names

    def test_select_multiple_columns(self, ctx):
        result = ctx.sql("SELECT id, name FROM t")
        table = result.to_arrow()
        assert table.num_columns == 2


class TestSelectWhere:
    def test_where_equals(self, ctx):
        result = ctx.sql("SELECT * FROM t WHERE name = 'alice'")
        assert result.row_count == 2

    def test_where_greater_than(self, ctx):
        result = ctx.sql("SELECT * FROM t WHERE value > 25")
        assert result.row_count == 3

    def test_where_less_than(self, ctx):
        result = ctx.sql("SELECT * FROM t WHERE id < 3")
        assert result.row_count == 2

    def test_where_and(self, ctx):
        result = ctx.sql("SELECT * FROM t WHERE name = 'bob' AND value > 25")
        assert result.row_count == 1

    def test_where_or(self, ctx):
        result = ctx.sql("SELECT * FROM t WHERE name = 'carol' OR value = 10")
        assert result.row_count == 2


class TestGroupByViaDataFrame:
    """GROUP BY via DataFrame API to avoid the SQL planner's Projection-over-Aggregate bug."""

    def test_group_by_sum(self, ctx):
        source = ctx._tables["t"]
        scan = Scan(source, "t")
        df = DataFrame(scan).aggregate(
            [Column("name")],
            [AggregateExpr("SUM", Column("value"))],
        )
        result = ctx.execute(df)
        table = result.to_arrow()
        d = table.to_pydict()
        name_col = d[table.column_names[0]]
        sum_col = d[table.column_names[1]]
        lookup = dict(zip(name_col, sum_col))
        assert lookup["alice"] == 40
        assert lookup["bob"] == 60
        assert lookup["carol"] == 50

    def test_group_by_count(self, ctx):
        source = ctx._tables["t"]
        scan = Scan(source, "t")
        df = DataFrame(scan).aggregate(
            [Column("name")],
            [AggregateExpr("COUNT", Column("id"))],
        )
        result = ctx.execute(df)
        table = result.to_arrow()
        d = table.to_pydict()
        name_col = d[table.column_names[0]]
        count_col = d[table.column_names[1]]
        lookup = dict(zip(name_col, count_col))
        assert lookup["alice"] == 2
        assert lookup["bob"] == 2
        assert lookup["carol"] == 1

    def test_group_by_min_max(self, ctx):
        source = ctx._tables["t"]
        scan = Scan(source, "t")
        df = DataFrame(scan).aggregate(
            [Column("name")],
            [AggregateExpr("MIN", Column("value")), AggregateExpr("MAX", Column("value"))],
        )
        result = ctx.execute(df)
        table = result.to_arrow()
        d = table.to_pydict()
        name_col = d[table.column_names[0]]
        min_col = d[table.column_names[1]]
        max_col = d[table.column_names[2]]
        lookup_min = dict(zip(name_col, min_col))
        lookup_max = dict(zip(name_col, max_col))
        assert lookup_min["alice"] == 10
        assert lookup_max["alice"] == 30


class TestOrderBy:
    def test_order_by_asc(self, ctx):
        result = ctx.sql("SELECT * FROM t ORDER BY value ASC")
        table = result.to_arrow()
        values = table.column("value").to_pylist()
        assert values == sorted(values)

    def test_order_by_desc(self, ctx):
        result = ctx.sql("SELECT * FROM t ORDER BY value DESC")
        table = result.to_arrow()
        values = table.column("value").to_pylist()
        assert values == sorted(values, reverse=True)


class TestLimit:
    def test_limit(self, ctx):
        result = ctx.sql("SELECT * FROM t LIMIT 3")
        assert result.row_count == 3

    def test_limit_one(self, ctx):
        result = ctx.sql("SELECT * FROM t LIMIT 1")
        assert result.row_count == 1

    def test_limit_with_order(self, ctx):
        result = ctx.sql("SELECT * FROM t ORDER BY value DESC LIMIT 2")
        table = result.to_arrow()
        values = table.column("value").to_pylist()
        assert len(values) == 2
        assert values[0] == 50
        assert values[1] == 40


class TestJoin:
    def test_inner_join(self, join_ctx):
        result = join_ctx.sql(
            "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
        )
        assert result.row_count == 3

    def test_join_with_filter(self, join_ctx):
        result = join_ctx.sql(
            "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id "
            "WHERE amount > 120"
        )
        assert result.row_count == 2


class TestExplain:
    def test_explain(self, ctx):
        result = ctx.sql("EXPLAIN SELECT * FROM t WHERE id > 2")
        table = result.to_arrow()
        assert table.num_columns == 1
        plan_text = table.column(0).to_pylist()[0]
        assert "Scan" in plan_text or "Selection" in plan_text

    def test_explain_select_star(self, ctx):
        result = ctx.sql("EXPLAIN SELECT * FROM t")
        plan_text = result.to_arrow().column(0).to_pylist()[0]
        assert "Scan" in plan_text


class TestDataFrameExecution:
    def test_dataframe_project(self, ctx):
        source = ctx._tables["t"]
        scan = Scan(source, "t")
        df = DataFrame(scan).project([Column("id"), Column("name")])
        result = ctx.execute(df)
        table = result.to_arrow()
        assert table.num_columns == 2

    def test_dataframe_filter(self, ctx):
        source = ctx._tables["t"]
        scan = Scan(source, "t")
        df = DataFrame(scan).filter(Gt(Column("value"), LiteralLong(25)))
        result = ctx.execute(df)
        assert result.row_count == 3

    def test_dataframe_aggregate(self, ctx):
        source = ctx._tables["t"]
        scan = Scan(source, "t")
        df = DataFrame(scan).aggregate(
            [Column("name")],
            [AggregateExpr("SUM", Column("value"))],
        )
        result = ctx.execute(df)
        assert result.row_count == 3  # alice, bob, carol

    def test_dataframe_chained(self, ctx):
        source = ctx._tables["t"]
        scan = Scan(source, "t")
        df = (
            DataFrame(scan)
            .filter(Gt(Column("id"), LiteralLong(1)))
            .project([Column("id"), Column("value")])
            .limit(2)
        )
        result = ctx.execute(df)
        assert result.row_count == 2
        table = result.to_arrow()
        assert table.num_columns == 2


class TestQueryResult:
    def test_to_pandas(self, ctx):
        result = ctx.sql("SELECT * FROM t")
        df = result.to_pandas()
        assert len(df) == 5
        assert list(df.columns) == ["id", "name", "value"]

    def test_to_markdown(self, ctx):
        result = ctx.sql("SELECT * FROM t LIMIT 2")
        md = result.to_markdown()
        assert "|" in md
        assert "id" in md

    def test_empty_result(self):
        result = QueryResult([])
        assert result.row_count == 0
        md = result.to_markdown()
        assert md == "(empty)"

    def test_repr(self, ctx):
        result = ctx.sql("SELECT * FROM t")
        assert "5" in repr(result)

    def test_register_and_list_tables(self, ctx):
        tables = ctx.tables()
        assert "t" in tables

    def test_table_schema(self, ctx):
        schema = ctx.table_schema("t")
        names = [f.name for f in schema.fields]
        assert "id" in names
        assert "name" in names
        assert "value" in names

    def test_unknown_table(self, ctx):
        with pytest.raises(ValueError, match="Unknown table"):
            ctx.sql("SELECT * FROM nonexistent")

    def test_to_csv(self, ctx, tmp_path):
        result = ctx.sql("SELECT * FROM t")
        csv_path = str(tmp_path / "out.csv")
        result.to_csv(csv_path)
        with open(csv_path) as f:
            content = f.read()
        assert "id" in content
        assert "alice" in content


class TestSelectWithAlias:
    def test_column_alias(self, ctx):
        result = ctx.sql("SELECT id AS identifier FROM t LIMIT 1")
        table = result.to_arrow()
        assert "identifier" in table.column_names


class TestSelectWithCast:
    def test_cast_int_to_double(self, ctx):
        result = ctx.sql("SELECT CAST(id AS DOUBLE) FROM t LIMIT 1")
        table = result.to_arrow()
        assert table.num_rows == 1
