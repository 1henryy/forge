import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from forge import ExecutionContext


def main() -> None:
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    data_path = os.path.join(base, "data", "trips.csv")

    if not os.path.exists(data_path):
        print(f"Generating sample data at {data_path} ...")
        sys.path.insert(0, os.path.join(base, "data"))
        from generate_sample_data import generate_trips
        generate_trips(data_path)

    ctx = ExecutionContext()
    ctx.register_csv("trips", data_path)

    print("=== SELECT * FROM trips LIMIT 5 ===")
    result = ctx.sql("SELECT * FROM trips LIMIT 5")
    result.show()
    print()

    print("=== WHERE fare_amount > 50 ===")
    result = ctx.sql(
        "SELECT trip_id, vendor_id, fare_amount "
        "FROM trips WHERE fare_amount > 50"
    )
    result.show()
    print()

    print("=== GROUP BY vendor_id ===")
    result = ctx.sql(
        "SELECT vendor_id, SUM(fare_amount), COUNT(trip_id) "
        "FROM trips GROUP BY vendor_id"
    )
    result.show()
    print()

    print("=== ORDER BY fare_amount DESC LIMIT 10 ===")
    result = ctx.sql(
        "SELECT trip_id, fare_amount, tip_amount "
        "FROM trips ORDER BY fare_amount DESC LIMIT 10"
    )
    result.show()
    print()

    print("=== EXPLAIN ===")
    result = ctx.sql(
        "EXPLAIN SELECT vendor_id, SUM(fare_amount) "
        "FROM trips WHERE passenger_count > 2 GROUP BY vendor_id"
    )
    result.show()


if __name__ == "__main__":
    main()
