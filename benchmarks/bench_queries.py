import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

QUERIES = {
    "scan": "SELECT * FROM orders LIMIT 1000",
    "filter": "SELECT order_id, amount FROM orders WHERE amount > 2000",
    "aggregate": "SELECT customer_id, SUM(amount), COUNT(order_id) FROM orders GROUP BY customer_id",
    "multi_agg": (
        "SELECT order_id, SUM(quantity), SUM(price), COUNT(lineitem_id) "
        "FROM lineitem GROUP BY order_id"
    ),
}

PANDAS_EQUIVALENTS = {
    "scan": lambda c, o, l: o.head(1000),
    "filter": lambda c, o, l: o.loc[o["amount"] > 2000, ["order_id", "amount"]],
    "aggregate": lambda c, o, l: o.groupby("customer_id").agg(
        sum_amount=("amount", "sum"), count_order=("order_id", "count")
    ).reset_index(),
    "multi_agg": lambda c, o, l: l.groupby("order_id").agg(
        sum_qty=("quantity", "sum"), sum_price=("price", "sum"), cnt=("lineitem_id", "count")
    ).reset_index(),
}


def ensure_data() -> None:
    if not os.path.exists(os.path.join(DATA_DIR, "customers.csv")):
        print("Benchmark data not found. Generating ...")
        from generate_data import generate_all
        generate_all(DATA_DIR)
        print()


def bench_forge() -> dict[str, float]:
    from forge import ExecutionContext

    ctx = ExecutionContext()
    ctx.register_csv("customers", os.path.join(DATA_DIR, "customers.csv"))
    ctx.register_csv("orders", os.path.join(DATA_DIR, "orders.csv"))
    ctx.register_csv("lineitem", os.path.join(DATA_DIR, "lineitem.csv"))

    results: dict[str, float] = {}
    for name, sql in QUERIES.items():
        try:
            start = time.perf_counter()
            result = ctx.sql(sql)
            _ = result.row_count
            elapsed = time.perf_counter() - start
            results[name] = elapsed
        except Exception as e:
            results[name] = -1
            print(f"  [forge] {name} failed: {e}")
    return results


def bench_pandas() -> dict[str, float]:
    try:
        import pandas as pd
    except ImportError:
        return {}

    customers = pd.read_csv(os.path.join(DATA_DIR, "customers.csv"))
    orders = pd.read_csv(os.path.join(DATA_DIR, "orders.csv"))
    lineitem = pd.read_csv(os.path.join(DATA_DIR, "lineitem.csv"))

    results: dict[str, float] = {}
    for name, fn in PANDAS_EQUIVALENTS.items():
        try:
            start = time.perf_counter()
            _ = fn(customers, orders, lineitem)
            elapsed = time.perf_counter() - start
            results[name] = elapsed
        except Exception as e:
            results[name] = -1
            print(f"  [pandas] {name} failed: {e}")
    return results


def bench_duckdb() -> dict[str, float]:
    try:
        import duckdb
    except ImportError:
        return {}

    con = duckdb.connect()
    con.execute(f"CREATE TABLE customers AS SELECT * FROM read_csv_auto('{os.path.join(DATA_DIR, 'customers.csv')}')")
    con.execute(f"CREATE TABLE orders AS SELECT * FROM read_csv_auto('{os.path.join(DATA_DIR, 'orders.csv')}')")
    con.execute(f"CREATE TABLE lineitem AS SELECT * FROM read_csv_auto('{os.path.join(DATA_DIR, 'lineitem.csv')}')")

    duckdb_queries = {
        "scan": "SELECT * FROM orders LIMIT 1000",
        "filter": "SELECT order_id, amount FROM orders WHERE amount > 2000",
        "aggregate": "SELECT customer_id, SUM(amount), COUNT(order_id) FROM orders GROUP BY customer_id",
        "multi_agg": (
            "SELECT order_id, SUM(quantity), SUM(price), COUNT(lineitem_id) "
            "FROM lineitem GROUP BY order_id"
        ),
    }

    results: dict[str, float] = {}
    for name, sql in duckdb_queries.items():
        try:
            start = time.perf_counter()
            con.execute(sql).fetchall()
            elapsed = time.perf_counter() - start
            results[name] = elapsed
        except Exception as e:
            results[name] = -1
            print(f"  [duckdb] {name} failed: {e}")
    con.close()
    return results


def format_time(t: float) -> str:
    if t < 0:
        return "FAIL"
    if t < 0.001:
        return f"{t * 1_000_000:.0f} us"
    if t < 1.0:
        return f"{t * 1_000:.1f} ms"
    return f"{t:.2f} s"


def print_table(forge_r: dict, pandas_r: dict, duckdb_r: dict) -> None:
    query_names = list(QUERIES.keys())
    engines = ["Forge"]
    all_results = [forge_r]
    if pandas_r:
        engines.append("pandas")
        all_results.append(pandas_r)
    if duckdb_r:
        engines.append("DuckDB")
        all_results.append(duckdb_r)

    col_w = max(12, max(len(e) for e in engines) + 2)
    name_w = max(len(n) for n in query_names) + 2

    header = f"{'Query':<{name_w}}"
    for eng in engines:
        header += f"{eng:>{col_w}}"
    sep = "-" * len(header)

    print(sep)
    print(header)
    print(sep)

    for name in query_names:
        row = f"{name:<{name_w}}"
        for r in all_results:
            t = r.get(name)
            val = format_time(t) if t is not None else "N/A"
            row += f"{val:>{col_w}}"
        print(row)
    print(sep)


def main() -> None:
    ensure_data()

    print("Benchmarking Forge ...")
    forge_r = bench_forge()

    print("Benchmarking pandas ...")
    pandas_r = bench_pandas()
    if not pandas_r:
        print("  (pandas not installed, skipping)")

    print("Benchmarking DuckDB ...")
    duckdb_r = bench_duckdb()
    if not duckdb_r:
        print("  (duckdb not installed, skipping)")

    print()
    print("=== Benchmark Results ===")
    print(f"Data: customers=10k, orders=100k, lineitem=500k rows")
    print()
    print_table(forge_r, pandas_r, duckdb_r)
    print()
    print("Note: Forge is an educational engine. Production engines like DuckDB")
    print("use vectorized execution, JIT compilation, and other optimizations.")


if __name__ == "__main__":
    main()
