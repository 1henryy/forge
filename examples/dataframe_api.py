import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from forge import ExecutionContext
from forge.logicalplan.expressions import (
    col, lit, sum_expr, count_expr, avg_expr, Gt, Column, LiteralLong, LiteralDouble,
)


def main() -> None:
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    data_path = os.path.join(base, "data", "trips.csv")

    if not os.path.exists(data_path):
        print(f"Generating sample data at {data_path} ...")
        sys.path.insert(0, os.path.join(base, "data"))
        from generate_sample_data import generate_trips
        generate_trips(data_path)

    ctx = ExecutionContext()
    df = ctx.csv(data_path)

    print("=== Filter + Project ===")
    df2 = (
        df.filter(Gt(col("fare_amount"), lit(50)))
          .project([col("trip_id"), col("vendor_id"), col("fare_amount")])
          .limit(10)
    )
    result = ctx.execute(df2)
    result.show()
    print()

    print("=== Aggregate: SUM, AVG, COUNT by vendor_id ===")
    df3 = df.aggregate(
        [col("vendor_id")],
        [sum_expr(col("fare_amount")), avg_expr(col("tip_amount")), count_expr(col("trip_id"))],
    )
    result = ctx.execute(df3)
    result.show()
    print()

    print("=== Sort by trip_distance DESC, LIMIT 5 ===")
    df4 = (
        df.project([col("trip_id"), col("trip_distance"), col("fare_amount")])
          .sort([(col("trip_distance"), False)])
          .limit(5)
    )
    result = ctx.execute(df4)
    result.show()
    print()

    print("=== Logical plan ===")
    df5 = (
        df.filter(Gt(col("passenger_count"), lit(2)))
          .aggregate([col("vendor_id")], [sum_expr(col("fare_amount"))])
    )
    print(df5.logical_plan().format())


if __name__ == "__main__":
    main()
