# Forge

A SQL query engine written in Python, built on Apache Arrow.

Built following the design principles and concepts described in
[*How Query Engines Work*](https://leanpub.com/how-query-engines-work) by Andy Grove.

## Architecture

Forge follows a standard query engine pipeline. A SQL string is tokenized and
parsed into an abstract syntax tree, which is then converted into a logical
plan, a tree of relational operators like Scan, Projection, Selection,
Aggregate, Join, Sort, and Limit. The optimizer rewrites this plan by pushing
projections and filters closer to the data source and folding constant
expressions. The logical plan is then translated into a physical plan where
each operator pulls data from its children one batch at a time using Apache
Arrow's columnar format. The execution engine runs the physical plan and
returns the results.

## Features

- **SQL Parser** - Hand-written recursive-descent parser supporting SELECT,
  WHERE, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, and EXPLAIN
- **DataFrame API** - Programmatic query building with filter, project,
  aggregate, sort, and limit operations
- **Query Optimizer** - Projection pushdown, filter pushdown, and constant
  folding passes
- **Physical Execution** - Iterator-based execution with hash aggregation,
  hash joins, and sort operators
- **Apache Arrow Backend** - Columnar in-memory format via PyArrow for
  efficient data processing
- **Data Sources** - CSV, Parquet, and in-memory table support
- **Parallel Execution** - Partitioned scans and parallel aggregation
- **Distributed Execution** - Coordinator/worker architecture over TCP
  sockets with JSON wire protocol
- **Interactive REPL** - Command-line interface with syntax highlighting
  and history (via prompt\_toolkit)

## Installation

```bash
# From the project root
pip install -e .

# With development/benchmark dependencies
pip install -e ".[dev]"
```

Requirements: Python 3.10+ and PyArrow.

## Quick Start

```python
from forge import ExecutionContext

ctx = ExecutionContext()

# Load data
ctx.register_csv("trips", "data/trips.csv")

# Run a SQL query
result = ctx.sql("""
    SELECT vendor_id, SUM(fare_amount), COUNT(trip_id)
    FROM trips
    WHERE passenger_count > 1
    GROUP BY vendor_id
""")
result.show()
```

Output:

```
vendor_id | SUM(#fare_amount) | COUNT(#trip_id)
----------+-------------------+----------------
1         | 12543.21          | 258
2         | 13102.87          | 263

(2 rows)
```

## API Reference

### SQL API

Register data sources, then execute SQL strings:

```python
from forge import ExecutionContext

ctx = ExecutionContext()

# Register tables from various sources
ctx.register_csv("trips", "data/trips.csv")
ctx.register_parquet("events", "data/events.parquet")
ctx.register_memory("users", {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
})

# Query
result = ctx.sql("SELECT * FROM trips WHERE fare_amount > 20 LIMIT 10")
result.show()              # Print formatted table
df = result.to_pandas()    # Convert to pandas DataFrame
table = result.to_arrow()  # Get PyArrow Table
result.to_csv("out.csv")   # Write to CSV
```

Supported SQL syntax:

```sql
SELECT col1, col2, SUM(col3), COUNT(*)
FROM table1
JOIN table2 ON table1.id = table2.id
WHERE col1 > 10 AND col2 = 'value'
GROUP BY col1, col2
HAVING SUM(col3) > 100
ORDER BY col1 DESC
LIMIT 50
```

Aggregate functions: `SUM`, `COUNT`, `MIN`, `MAX`, `AVG`.

### DataFrame API

Build queries programmatically using expression constructors:

```python
from forge import ExecutionContext
from forge.logicalplan.expressions import (
    col, lit, sum_expr, avg_expr, count_expr, Gt,
)

ctx = ExecutionContext()
df = ctx.csv("data/trips.csv")

# Chain operations
result = ctx.execute(
    df.filter(Gt(col("fare_amount"), lit(10)))
      .project([col("vendor_id"), col("fare_amount")])
      .sort([(col("fare_amount"), False)])  # False = DESC
      .limit(20)
)
result.show()

# Aggregation
result = ctx.execute(
    df.aggregate(
        [col("vendor_id")],                           # GROUP BY
        [sum_expr(col("fare_amount")),                 # SUM
         avg_expr(col("tip_amount")),                  # AVG
         count_expr(col("trip_id"))],                  # COUNT
    )
)
result.show()
```

Available expression constructors:

| Function | Description |
|---|---|
| `col(name)` | Column reference |
| `lit(value)` | Literal value (int, float, str, bool) |
| `sum_expr(expr)` | SUM aggregate |
| `count_expr(expr)` | COUNT aggregate |
| `avg_expr(expr)` | AVG aggregate |
| `min_expr(expr)` | MIN aggregate |
| `max_expr(expr)` | MAX aggregate |
| `Gt(left, right)` | Greater than |
| `Lt(left, right)` | Less than |
| `Eq(left, right)` | Equal |
| `And(left, right)` | Logical AND |
| `Or(left, right)` | Logical OR |

### REPL

Start the interactive SQL shell:

```bash
python -m forge
# or, if installed:
forge-cli
```

```
Forge SQL Engine v0.1.0
Type .help for commands, .quit to exit

forge> .load trips data/trips.csv
Loaded 'data/trips.csv' as 'trips'

forge> SELECT vendor_id, AVG(fare_amount) FROM trips GROUP BY vendor_id;
vendor_id | AVG(#fare_amount)
----------+------------------
1         | 24.87
2         | 25.12

(2 rows)
Time: 0.042s

forge> .tables
  trips

forge> .schema trips
  trip_id: Int64
  vendor_id: Int64
  passenger_count: Int64
  trip_distance: Float64
  fare_amount: Float64
  tip_amount: Float64

forge> .quit
Bye!
```

### EXPLAIN

Inspect query plans with the `EXPLAIN` prefix:

```python
from forge import ExecutionContext

ctx = ExecutionContext()
ctx.register_csv("trips", "data/trips.csv")

result = ctx.sql(
    "EXPLAIN SELECT vendor_id, SUM(fare_amount) "
    "FROM trips WHERE passenger_count > 2 GROUP BY vendor_id"
)
result.show()
```

```
plan
----------------------------------------------
ProjectionExec(#0, #1)
  HashAggregateExec(groups=[#0], aggs=[SUM(#2)])
    SelectionExec(#1 greater #3)
      ScanExec(projection=[0, 1, 4])
```

## Running Examples

Generate sample data and run the examples:

```bash
# Generate sample trip data
python data/generate_sample_data.py

# Run examples
python examples/basic_sql.py
python examples/dataframe_api.py
python examples/joins_and_aggregates.py
python examples/parallel_execution.py
python examples/distributed_demo.py
```

## Benchmarks

Generate benchmark data and compare Forge against pandas and DuckDB:

```bash
# Generate TPC-H-like benchmark data (10k customers, 100k orders, 500k line items)
python benchmarks/generate_data.py

# Run benchmarks
python benchmarks/bench_queries.py
# or: make bench
```

Sample output:

```
Query             Forge      pandas      DuckDB
-----------------------------------------------
scan            38.0 ms      1.6 ms      5.8 ms
filter         273.0 ms      3.7 ms      7.3 ms
aggregate      307.0 ms      7.3 ms      3.3 ms
multi_agg        2.07 s     19.1 ms     43.5 ms
```

Forge is an educational engine and is not optimized for production workloads.
The benchmark exists to illustrate the performance characteristics of a
query engine built from scratch compared to mature systems.

## Acknowledgments

This project is inspired by and follows the architecture described in
[*How Query Engines Work*](https://leanpub.com/how-query-engines-work) by
Andy Grove, the creator of Apache Arrow DataFusion.
