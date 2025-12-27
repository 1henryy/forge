import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from forge import ExecutionContext


def main() -> None:
    ctx = ExecutionContext()

    ctx.register_memory("customers", {
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "region": ["East", "West", "East", "West", "East"],
    })

    ctx.register_memory("orders", {
        "order_id": [101, 102, 103, 104, 105, 106, 107, 108],
        "customer_id": [1, 1, 2, 3, 3, 3, 4, 5],
        "amount": [250.0, 130.0, 480.0, 75.0, 210.0, 340.0, 560.0, 90.0],
        "product": ["Widget", "Gadget", "Widget", "Gizmo", "Widget", "Gadget", "Widget", "Gizmo"],
    })

    print("=== INNER JOIN customers and orders ===")
    result = ctx.sql(
        "SELECT name, order_id, amount "
        "FROM customers JOIN orders ON customer_id = customer_id"
    )
    result.show()
    print()

    print("=== Total spend per customer ===")
    result = ctx.sql(
        "SELECT name, SUM(amount), COUNT(order_id) "
        "FROM customers JOIN orders ON customer_id = customer_id "
        "GROUP BY name"
    )
    result.show()
    print()

    print("=== Total spend per region ===")
    result = ctx.sql(
        "SELECT region, SUM(amount) "
        "FROM customers JOIN orders ON customer_id = customer_id "
        "GROUP BY region"
    )
    result.show()
    print()

    print("=== Filtered join: amount > 200 ===")
    result = ctx.sql(
        "SELECT name, product, amount "
        "FROM customers JOIN orders ON customer_id = customer_id "
        "WHERE amount > 200"
    )
    result.show()


if __name__ == "__main__":
    main()
