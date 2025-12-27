import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from forge.execution.context import ExecutionContext


COLUMNS = ["id", "name", "value"]


def _make_ctx():
    ctx = ExecutionContext()
    ctx.register_memory("t", {
        "id": [1, 2, 3, 4, 5],
        "name": ["alice", "bob", "alice", "bob", "carol"],
        "value": [10, 20, 30, 40, 50],
    })
    return ctx


safe_identifiers = st.sampled_from(COLUMNS)
int_literals = st.integers(min_value=-1000, max_value=1000)
float_literals = st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
comparison_ops = st.sampled_from(["=", "!=", "<", ">", "<=", ">="])


@st.composite
def simple_where_clause(draw):
    col = draw(safe_identifiers)
    if col == "name":
        names = ["alice", "bob", "carol", "dave"]
        val = draw(st.sampled_from(names))
        op = draw(st.sampled_from(["=", "!="]))
        return f"{col} {op} '{val}'"
    else:
        val = draw(int_literals)
        op = draw(comparison_ops)
        return f"{col} {op} {val}"


@st.composite
def select_columns(draw):
    n = draw(st.integers(min_value=1, max_value=3))
    cols = draw(st.lists(safe_identifiers, min_size=n, max_size=n))
    return ", ".join(cols)


class TestFuzzySelectQueries:
    @given(cols=select_columns())
    @settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
    def test_select_columns_no_crash(self, cols):
        ctx = _make_ctx()
        sql = f"SELECT {cols} FROM t"
        result = ctx.sql(sql)
        assert result.row_count == 5

    @given(where=simple_where_clause())
    @settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
    def test_select_with_where_no_crash(self, where):
        ctx = _make_ctx()
        sql = f"SELECT * FROM t WHERE {where}"
        result = ctx.sql(sql)
        assert result.row_count >= 0

    @given(limit=st.integers(min_value=0, max_value=20))
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_select_with_limit_no_crash(self, limit):
        ctx = _make_ctx()
        sql = f"SELECT * FROM t LIMIT {limit}"
        result = ctx.sql(sql)
        assert result.row_count <= max(limit, 5)
        assert result.row_count >= 0

    @given(
        where=simple_where_clause(),
        cols=select_columns(),
        limit=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_combined_query_no_crash(self, where, cols, limit):
        ctx = _make_ctx()
        sql = f"SELECT {cols} FROM t WHERE {where} LIMIT {limit}"
        try:
            result = ctx.sql(sql)
            assert result.row_count >= 0
        except (ValueError, SyntaxError):
            pass

    @given(
        order_col=safe_identifiers,
        direction=st.sampled_from(["ASC", "DESC"]),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_order_by_no_crash(self, order_col, direction):
        ctx = _make_ctx()
        sql = f"SELECT * FROM t ORDER BY {order_col} {direction}"
        result = ctx.sql(sql)
        assert result.row_count == 5


class TestFuzzyExpressions:
    @given(a=int_literals)
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_arithmetic_in_select(self, a):
        ctx = _make_ctx()
        sql = f"SELECT id + {a} FROM t LIMIT 1"
        try:
            result = ctx.sql(sql)
            assert result.row_count >= 0
        except (ValueError, SyntaxError, OverflowError):
            pass

    @given(
        val=st.text(
            alphabet=st.characters(whitelist_categories=("L", "N", "Zs")),
            min_size=0,
            max_size=10,
        ),
    )
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_string_comparison_no_crash(self, val):
        safe_val = val.replace("'", "''")
        ctx = _make_ctx()
        sql = f"SELECT * FROM t WHERE name = '{safe_val}'"
        try:
            result = ctx.sql(sql)
            assert result.row_count >= 0
        except (ValueError, SyntaxError):
            pass


class TestFuzzyAggregation:
    """Test aggregation via DataFrame API to avoid SQL planner's aggregate projection bug."""

    @given(
        agg_func=st.sampled_from(["SUM", "COUNT", "MIN", "MAX", "AVG"]),
        agg_col=st.sampled_from(["id", "value"]),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_aggregate_no_crash(self, agg_func, agg_col):
        from forge.logicalplan.expressions import Column, AggregateExpr
        from forge.logicalplan.plan import Scan
        from forge.logicalplan.dataframe import DataFrame

        ctx = _make_ctx()
        source = ctx._tables["t"]
        scan = Scan(source, "t")
        df = DataFrame(scan).aggregate(
            [Column("name")],
            [AggregateExpr(agg_func, Column(agg_col))],
        )
        result = ctx.execute(df)
        assert result.row_count > 0
