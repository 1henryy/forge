import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from forge.distributed.worker import Worker
from forge.distributed.coordinator import Coordinator


def main() -> None:
    worker = Worker(host="localhost", port=19876)
    worker.register_memory("products", {
        "product_id": [1, 2, 3, 4, 5],
        "name": ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"],
        "price": [9.99, 24.99, 14.50, 3.75, 49.99],
        "category": ["A", "B", "A", "C", "B"],
    })

    print("Starting worker on localhost:19876 ...")
    thread = worker.start_background()
    time.sleep(0.5)

    coordinator = Coordinator()
    coordinator.add_worker("localhost", 19876)
    print(f"Coordinator has {len(coordinator.workers)} worker(s)\n")

    print("=== SELECT * FROM products ===")
    result = coordinator.execute("SELECT * FROM products")
    result.show()
    print()

    print("=== Filter: price > 10 ===")
    result = coordinator.execute(
        "SELECT name, price FROM products WHERE price > 10"
    )
    result.show()
    print()

    print("=== Aggregate: SUM(price) by category ===")
    result = coordinator.execute(
        "SELECT category, SUM(price) FROM products GROUP BY category"
    )
    result.show()
    print()

    print("Shutting down worker ...")
    coordinator.shutdown_workers()
    time.sleep(0.5)
    print("Done.")


if __name__ == "__main__":
    main()
