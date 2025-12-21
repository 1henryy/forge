import pytest

from forge.datatypes import DataType, Field, Schema
from forge.datasources.memory_source import MemorySource
from forge.logicalplan.expressions import (
    Column, LiteralLong, LiteralDouble, LiteralBoolean,
    Add, Subtract, Multiply, Divide, Eq, And, Or, Not,
    AggregateExpr,
)
from forge.logicalplan.plan import (
    Scan, Projection, Selection, Aggregate, Sort, Limit,
)
from forge.optimizer.constant_folding import ConstantFolding
from forge.optimizer.filter_pushdown import FilterPushdown
from forge.optimizer.projection_pushdown import ProjectionPushdown
from forge.optimizer.optimizer import Optimizer


@pytest.fixture
def source():
    return MemorySource.from_pydict({
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "value": [10, 20, 30],
    })


class TestConstantFolding:
    def test_add_integers(self):
        cf = ConstantFolding()
        scan_src = MemorySource.from_pydict({"x": [1]})
        scan = Scan(scan_src, "t")
        proj = Projection(scan, [Add(LiteralLong(1), LiteralLong(2))])
        result = cf.optimize(proj)
        assert isinstance(result, Projection)
        folded = result.exprs[0]
        assert isinstance(folded, LiteralLong)
        assert folded.value == 3

    def test_multiply_integers(self):
        cf = ConstantFolding()
        scan_src = MemorySource.from_pydict({"x": [1]})
        scan = Scan(scan_src, "t")
        proj = Projection(scan, [Multiply(LiteralLong(4), LiteralLong(5))])
        result = cf.optimize(proj)
        folded = result.exprs[0]
        assert isinstance(folded, LiteralLong)
        assert folded.value == 20

    def test_subtract_integers(self):
        cf = ConstantFolding()
        scan_src = MemorySource.from_pydict({"x": [1]})
        scan = Scan(scan_src, "t")
        proj = Projection(scan, [Subtract(LiteralLong(10), LiteralLong(3))])
        result = cf.optimize(proj)
        folded = result.exprs[0]
        assert isinstance(folded, LiteralLong)
        assert folded.value == 7

    def test_division_yields_double(self):
        cf = ConstantFolding()
        scan_src = MemorySource.from_pydict({"x": [1]})
        scan = Scan(scan_src, "t")
        proj = Projection(scan, [Divide(LiteralLong(10), LiteralLong(3))])
        result = cf.optimize(proj)
        folded = result.exprs[0]
        assert isinstance(folded, LiteralDouble)

    def test_mixed_float_int(self):
        cf = ConstantFolding()
        scan_src = MemorySource.from_pydict({"x": [1]})
        scan = Scan(scan_src, "t")
        proj = Projection(scan, [Add(LiteralLong(1), LiteralDouble(2.5))])
        result = cf.optimize(proj)
        folded = result.exprs[0]
        assert isinstance(folded, LiteralDouble)
        assert folded.value == 3.5

    def test_comparison_folding(self):
        cf = ConstantFolding()
        scan_src = MemorySource.from_pydict({"x": [1]})
        scan = Scan(scan_src, "t")
        proj = Projection(scan, [Eq(LiteralLong(1), LiteralLong(1))])
        result = cf.optimize(proj)
        folded = result.exprs[0]
        assert isinstance(folded, LiteralBoolean)
        assert folded.value is True

    def test_true_and_x(self, source):
        cf = ConstantFolding()
        scan = Scan(source, "t")
        sel = Selection(scan, And(LiteralBoolean(True), Column("id")))
        result = cf.optimize(sel)
        assert isinstance(result, Selection)
        assert isinstance(result.expr, Column)
        assert result.expr.name == "id"

    def test_false_and_x(self, source):
        cf = ConstantFolding()
        scan = Scan(source, "t")
        sel = Selection(scan, And(LiteralBoolean(False), Column("id")))
        result = cf.optimize(sel)
        assert isinstance(result, Selection)
        assert isinstance(result.expr, LiteralBoolean)
        assert result.expr.value is False

    def test_true_or_x_becomes_true_and_selection_eliminated(self, source):
        cf = ConstantFolding()
        scan = Scan(source, "t")
        sel = Selection(scan, Or(LiteralBoolean(True), Column("id")))
        result = cf.optimize(sel)
        # OR(True, x) -> LiteralBoolean(True), then Selection(scan, True) -> scan
        assert isinstance(result, Scan)

    def test_false_or_x(self, source):
        cf = ConstantFolding()
        scan = Scan(source, "t")
        sel = Selection(scan, Or(LiteralBoolean(False), Column("id")))
        result = cf.optimize(sel)
        assert isinstance(result, Selection)
        assert isinstance(result.expr, Column)

    def test_not_true(self):
        cf = ConstantFolding()
        scan_src = MemorySource.from_pydict({"x": [1]})
        scan = Scan(scan_src, "t")
        proj = Projection(scan, [Not(LiteralBoolean(True))])
        result = cf.optimize(proj)
        folded = result.exprs[0]
        assert isinstance(folded, LiteralBoolean)
        assert folded.value is False

    def test_selection_with_literal_true_removed(self, source):
        cf = ConstantFolding()
        scan = Scan(source, "t")
        sel = Selection(scan, LiteralBoolean(True))
        result = cf.optimize(sel)
        assert isinstance(result, Scan)

    def test_nested_folding(self):
        cf = ConstantFolding()
        scan_src = MemorySource.from_pydict({"x": [1]})
        scan = Scan(scan_src, "t")
        expr = Add(LiteralLong(1), Multiply(LiteralLong(2), LiteralLong(3)))
        proj = Projection(scan, [expr])
        result = cf.optimize(proj)
        folded = result.exprs[0]
        assert isinstance(folded, LiteralLong)
        assert folded.value == 7


class TestFilterPushdown:
    def test_push_through_projection(self, source):
        fp = FilterPushdown()
        scan = Scan(source, "t")
        proj = Projection(scan, [Column("id"), Column("name")])
        sel = Selection(proj, Eq(Column("id"), LiteralLong(1)))
        result = fp.optimize(sel)
        assert isinstance(result, Projection)
        assert isinstance(result.input, Selection)
        assert isinstance(result.input.input, Scan)

    def test_filter_stays_when_columns_not_in_child(self, source):
        fp = FilterPushdown()
        scan = Scan(source, "t")
        from forge.logicalplan.expressions import Alias
        proj = Projection(scan, [Alias(Column("id"), "new_id")])
        sel = Selection(proj, Eq(Column("new_id"), LiteralLong(1)))
        result = fp.optimize(sel)
        assert isinstance(result, Selection)

    def test_push_through_sort(self, source):
        fp = FilterPushdown()
        scan = Scan(source, "t")
        sort = Sort(scan, [(Column("id"), True)])
        sel = Selection(sort, Eq(Column("id"), LiteralLong(1)))
        result = fp.optimize(sel)
        assert isinstance(result, Sort)
        assert isinstance(result.input, Selection)


class TestProjectionPushdown:
    def test_adds_projection_to_scan(self, source):
        pp = ProjectionPushdown()
        scan = Scan(source, "t")
        proj = Projection(scan, [Column("id")])
        result = pp.optimize(proj)
        assert isinstance(result, Projection)
        inner = result.input
        assert isinstance(inner, Scan)
        assert inner.projection is not None
        assert 0 in inner.projection

    def test_includes_filter_columns(self, source):
        pp = ProjectionPushdown()
        scan = Scan(source, "t")
        sel = Selection(scan, Eq(Column("name"), LiteralLong(1)))
        proj = Projection(sel, [Column("id")])
        result = pp.optimize(proj)
        assert isinstance(result, Projection)
        assert isinstance(result.input, Selection)
        inner_scan = result.input.input
        assert isinstance(inner_scan, Scan)
        assert inner_scan.projection is not None
        proj_indices = inner_scan.projection
        source_schema = source.schema()
        projected_names = {source_schema[i].name for i in proj_indices}
        assert "id" in projected_names
        assert "name" in projected_names


class TestFullOptimizer:
    def test_default_optimizer_pipeline(self, source):
        opt = Optimizer.default()
        scan = Scan(source, "t")
        proj = Projection(scan, [Column("id"), Column("name")])
        sel = Selection(proj, Eq(Column("id"), LiteralLong(1)))
        result = opt.optimize(sel)
        text = result.format()
        assert "Scan" in text

    def test_optimizer_with_constant_and_filter(self, source):
        opt = Optimizer.default()
        scan = Scan(source, "t")
        sel = Selection(scan, And(LiteralBoolean(True), Eq(Column("id"), LiteralLong(1))))
        proj = Projection(sel, [Column("id")])
        result = opt.optimize(proj)
        text = result.format()
        assert "Projection" in text
