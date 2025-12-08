from __future__ import annotations

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from typing import Iterator

from forge.datatypes import RecordBatch, Schema
from forge.physicalplan.plan import PhysicalPlan
from forge.physicalplan.scan_exec import ScanExec
from forge.physicalplan.hash_aggregate_exec import HashAggregateExec
from forge.physicalplan import expressions as pexpr
from forge.datasources.memory_source import MemorySource

from .partition import RoundRobinPartition


def _execute_partition(source_batches_bytes: bytes, schema_fields: list, projection) -> list:
    import pyarrow as pa
    from forge.datatypes import RecordBatch as RB

    reader = pa.ipc.open_stream(source_batches_bytes)
    batches = [RB.from_arrow(b) for b in reader]
    return [b.to_arrow().to_pydict() for b in batches]


class ParallelScanExec(PhysicalPlan):
    def __init__(
        self, source: ScanExec, num_partitions: int = 4,
    ) -> None:
        self._source = source
        self._num_partitions = num_partitions

    def schema(self) -> Schema:
        return self._source.schema()

    def execute(self) -> Iterator[RecordBatch]:
        all_batches = list(self._source.execute())
        if len(all_batches) <= 1 or self._num_partitions <= 1:
            yield from all_batches
            return

        partitioner = RoundRobinPartition()
        partitions = partitioner.partition(iter(all_batches), self._num_partitions)

        for part in partitions:
            yield from part

    def children(self) -> list[PhysicalPlan]:
        return [self._source]

    def __str__(self) -> str:
        return f"ParallelScan: partitions={self._num_partitions}"


class ParallelAggregateExec(PhysicalPlan):
    def __init__(
        self,
        input_plan: PhysicalPlan,
        group_exprs: list[pexpr.PhysicalExpr],
        agg_exprs: list[pexpr.AggregateExpr],
        output_schema: Schema,
        num_partitions: int = 4,
    ) -> None:
        self._input = input_plan
        self._group_exprs = group_exprs
        self._agg_exprs = agg_exprs
        self._schema = output_schema
        self._num_partitions = num_partitions

    def schema(self) -> Schema:
        return self._schema

    def execute(self) -> Iterator[RecordBatch]:
        all_batches = list(self._input.execute())
        if not all_batches:
            return

        partitioner = RoundRobinPartition()
        partitions = partitioner.partition(iter(all_batches), self._num_partitions)

        partial_results: list[RecordBatch] = []
        for part in partitions:
            if not part:
                continue
            source = MemorySource(self._input.schema(), part)
            from forge.physicalplan.scan_exec import ScanExec
            scan = ScanExec(source)
            agg = HashAggregateExec(scan, self._group_exprs, self._agg_exprs, self._schema)
            partial_results.extend(agg.execute())

        if not partial_results:
            return

        merge_source = MemorySource(self._schema, partial_results)
        merge_scan = ScanExec(merge_source)
        final_agg = HashAggregateExec(
            merge_scan, self._group_exprs, self._agg_exprs, self._schema,
        )
        yield from final_agg.execute()

    def children(self) -> list[PhysicalPlan]:
        return [self._input]

    def __str__(self) -> str:
        return f"ParallelAggregate: partitions={self._num_partitions}"
