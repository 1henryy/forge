import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from forge import ExecutionContext
from forge.datasources import MemorySource
from forge.physicalplan.scan_exec import ScanExec
from forge.parallel.executor import ParallelScanExec, ParallelAggregateExec
from forge.parallel.partition import RoundRobinPartition, HashPartition
from forge.physicalplan import expressions as pexpr
from forge.datatypes import DataType, Field, Schema


def main() -> None:
    ctx = ExecutionContext()

    data = {
        "region": (["East"] * 300 + ["West"] * 300 + ["North"] * 200 + ["South"] * 200),
        "sales": [float(i % 100 + 10) for i in range(1000)],
        "quantity": [i % 50 + 1 for i in range(1000)],
    }
    ctx.register_memory("sales", data)
    source = ctx._tables["sales"]

    print("=== Parallel Scan (4 partitions) ===")
    scan = ScanExec(source)
    parallel_scan = ParallelScanExec(scan, num_partitions=4)
    batches = list(parallel_scan.execute())
    total_rows = sum(b.row_count for b in batches)
    print(f"Total rows across {len(batches)} batch(es): {total_rows}")
    print()

    print("=== Round-Robin Partitioning ===")
    scan2 = ScanExec(source)
    all_batches = list(scan2.execute())
    rr = RoundRobinPartition()
    partitions = rr.partition(iter(all_batches), 4)
    for i, part in enumerate(partitions):
        rows = sum(b.row_count for b in part)
        print(f"  Partition {i}: {rows} rows across {len(part)} batch(es)")
    print()

    print("=== Hash Partitioning on 'region' (column 0) ===")
    scan3 = ScanExec(source)
    all_batches3 = list(scan3.execute())
    hp = HashPartition(key_index=0)
    partitions3 = hp.partition(iter(all_batches3), 4)
    for i, part in enumerate(partitions3):
        rows = sum(b.row_count for b in part)
        print(f"  Partition {i}: {rows} rows across {len(part)} batch(es)")
    print()

    print("=== Parallel Aggregate (SUM of sales by region) ===")
    scan4 = ScanExec(source)
    output_schema = Schema([
        Field("region", DataType.Utf8),
        Field("SUM(#sales)", DataType.Float64),
    ])
    par_agg = ParallelAggregateExec(
        input_plan=scan4,
        group_exprs=[pexpr.ColumnExpr(0)],
        agg_exprs=[pexpr.SumExpr(pexpr.ColumnExpr(1))],
        output_schema=output_schema,
        num_partitions=4,
    )
    from forge.execution.result import QueryResult
    result = QueryResult(list(par_agg.execute()))
    result.show()


if __name__ == "__main__":
    main()
