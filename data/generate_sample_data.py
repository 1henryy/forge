import csv
import os
import random
import sys


def generate_trips(path: str, n: int = 1000, seed: int = 42) -> None:
    random.seed(seed)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "trip_id", "vendor_id", "passenger_count",
            "trip_distance", "fare_amount", "tip_amount",
        ])
        for i in range(1, n + 1):
            vendor_id = random.choice([1, 2])
            passenger_count = random.randint(1, 6)
            trip_distance = round(random.uniform(0.5, 30.0), 2)
            fare_amount = round(2.50 + trip_distance * random.uniform(1.8, 3.2), 2)
            tip_amount = round(fare_amount * random.uniform(0.0, 0.30), 2)
            writer.writerow([
                i, vendor_id, passenger_count,
                trip_distance, fare_amount, tip_amount,
            ])
    print(f"Generated {n} rows -> {path}")


if __name__ == "__main__":
    base = os.path.dirname(os.path.abspath(__file__))
    output = os.path.join(base, "trips.csv")
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    generate_trips(output, n=count)
