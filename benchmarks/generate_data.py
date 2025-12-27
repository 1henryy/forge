import csv
import os
import random
import sys

REGIONS = ["North", "South", "East", "West", "Central"]
FIRST_NAMES = [
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mona", "Nick", "Olivia", "Pete",
]
LAST_NAMES = [
    "Smith", "Jones", "Brown", "Davis", "Clark", "Lewis", "Walker",
    "Hall", "Young", "King", "Wright", "Adams", "Nelson", "Hill",
]


def generate_customers(path: str, n: int = 10_000, seed: int = 42) -> None:
    random.seed(seed)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "name", "region"])
        for i in range(1, n + 1):
            name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
            region = random.choice(REGIONS)
            w.writerow([i, name, region])
    print(f"  customers: {n} rows -> {path}")


def generate_orders(path: str, n: int = 100_000, n_customers: int = 10_000, seed: int = 43) -> None:
    random.seed(seed)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "amount", "status"])
        statuses = ["pending", "shipped", "delivered", "returned"]
        for i in range(1, n + 1):
            cid = random.randint(1, n_customers)
            amount = round(random.uniform(5.0, 5000.0), 2)
            status = random.choice(statuses)
            w.writerow([i, cid, amount, status])
    print(f"  orders:    {n} rows -> {path}")


def generate_lineitem(path: str, n: int = 500_000, n_orders: int = 100_000, seed: int = 44) -> None:
    random.seed(seed)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["lineitem_id", "order_id", "quantity", "price"])
        for i in range(1, n + 1):
            oid = random.randint(1, n_orders)
            quantity = random.randint(1, 50)
            price = round(random.uniform(1.0, 500.0), 2)
            w.writerow([i, oid, quantity, price])
    print(f"  lineitem:  {n} rows -> {path}")


def generate_all(base_dir: str) -> None:
    os.makedirs(base_dir, exist_ok=True)
    print("Generating benchmark data ...")
    generate_customers(os.path.join(base_dir, "customers.csv"))
    generate_orders(os.path.join(base_dir, "orders.csv"))
    generate_lineitem(os.path.join(base_dir, "lineitem.csv"))
    print("Done.")


if __name__ == "__main__":
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    generate_all(base)
