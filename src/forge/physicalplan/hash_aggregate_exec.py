# __MARKER_0__
# __MARKER_1__
from typing import Any, Iterator

import pyarrow as pa

from forge.datatypes import Schema, RecordBatch, ArrowVector, to_arrow_type
from .plan import PhysicalPlan
from .expressions import PhysicalExpr, AggregateExpr, Accumulator, _to_arrow_array


class HashAggregateExec(PhysicalPlan):
    def __init__(
        self,
        input_plan: PhysicalPlan,
        group_exprs: list[PhysicalExpr],
        agg_exprs: list[AggregateExpr],
        schema: Schema,
    ) -> None:
        self._input: PhysicalPlan = input_plan
        self._group_exprs: list[PhysicalExpr] = group_exprs
        self._agg_exprs: list[AggregateExpr] = agg_exprs
        self._schema: Schema = schema

    def schema(self) -> Schema:
        return self._schema

    def execute(self) -> Iterator[RecordBatch]:
        groups: dict[tuple[Any, ...], list[Accumulator]] = {}

        for batch in self._input.execute():
            group_cols = [expr.evaluate(batch) for expr in self._group_exprs]
            agg_input_cols = [agg.input_expr().evaluate(batch) for agg in self._agg_exprs]

            for row_idx in range(batch.row_count):
                key = tuple(col.get_value(row_idx) for col in group_cols)

                if key not in groups:
                    groups[key] = [agg.create_accumulator() for agg in self._agg_exprs]

                accumulators = groups[key]
                for acc_idx, acc in enumerate(accumulators):
                    value = agg_input_cols[acc_idx].get_value(row_idx)
                    from forge.datatypes import LiteralVector, DataType
                    single = LiteralVector(value, agg_input_cols[acc_idx].dtype, 1)
                    acc.accumulate(single)

        num_group_cols = len(self._group_exprs)
        num_agg_cols = len(self._agg_exprs)
        num_cols = num_group_cols + num_agg_cols

        if not groups:
            arrays = [pa.array([], type=to_arrow_type(self._schema[i].data_type)) for i in range(num_cols)]
        else:
            columns: list[list[Any]] = [[] for _ in range(num_cols)]
            for key, accumulators in groups.items():
                for i, val in enumerate(key):
                    columns[i].append(val)
                for i, acc in enumerate(accumulators):
                    columns[num_group_cols + i].append(acc.final_value())
            arrays = [
                pa.array(columns[i], type=to_arrow_type(self._schema[i].data_type))
                for i in range(num_cols)
            ]

        arrow_schema = pa.schema([
            pa.field(self._schema[i].name, to_arrow_type(self._schema[i].data_type))
            for i in range(num_cols)
        ])
        arrow_batch = pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)
        yield RecordBatch.from_arrow(arrow_batch)

    def children(self) -> list[PhysicalPlan]:
        return [self._input]

    def __str__(self) -> str:
        groups_str = ", ".join(str(e) for e in self._group_exprs)
        aggs_str = ", ".join(str(e) for e in self._agg_exprs)
        return f"HashAggregateExec(groups=[{groups_str}], aggs=[{aggs_str}])"
