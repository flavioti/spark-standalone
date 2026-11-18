"""Generate small, reproducible sample datasets for Spark exercises.

Standard library only, so it runs with any Python 3. Output goes to ./data.

    python3 scripts/generate_sample_data.py
    python3 scripts/generate_sample_data.py --orders 500000   # larger run
"""

import argparse
import csv
import json
import random
from datetime import date, datetime, timedelta
from itertools import accumulate
from pathlib import Path

SEED = 42
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

CITIES = [
    ("São Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Salvador", "BA"),
    ("Recife", "PE"), ("Fortaleza", "CE"), ("Brasília", "DF"), ("Manaus", "AM"),
]
SEGMENTS = ["consumer", "small_business", "enterprise"]
CATEGORIES = {
    "electronics": (150, 4000),
    "books": (20, 200),
    "home": (30, 800),
    "fashion": (25, 500),
    "sports": (40, 900),
}
STATUSES = ["delivered"] * 8 + ["shipped", "cancelled", "returned"]
EVENT_TYPES = ["view", "view", "view", "add_to_cart", "purchase"]


def write_csv(path, header, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"  {path.relative_to(ROOT)}: {len(rows):,} rows")


def generate_customers(rng, n):
    rows = []
    for cid in range(1, n + 1):
        city, state = rng.choice(CITIES)
        signup = date(2021, 1, 1) + timedelta(days=rng.randint(0, 1800))
        rows.append([cid, f"customer_{cid:05d}", city, state,
                     rng.choice(SEGMENTS), signup.isoformat()])
    return ["customer_id", "name", "city", "state", "segment", "signup_date"], rows


def generate_products(rng, n):
    rows = []
    for pid in range(1, n + 1):
        category = rng.choice(list(CATEGORIES))
        low, high = CATEGORIES[category]
        price = round(rng.uniform(low, high), 2)
        rows.append([pid, f"{category}_item_{pid:04d}", category, price])
    return ["product_id", "product_name", "category", "unit_price"], rows


def generate_orders(rng, n_orders, n_customers, n_products):
    # Customer activity is skewed (a few customers order a lot), which makes
    # groupBy/join exercises show realistic data skew.
    weights = [1.0 / (i ** 0.8) for i in range(1, n_customers + 1)]
    # Precompute cumulative weights once; passing weights= on every call would
    # rebuild them for each order (O(orders * customers)).
    cum_weights = list(accumulate(weights))
    customer_ids = list(range(1, n_customers + 1))
    start = datetime(2023, 1, 1)
    rows = []
    for oid in range(1, n_orders + 1):
        ts = start + timedelta(seconds=rng.randint(0, 2 * 365 * 24 * 3600))
        rows.append([
            oid,
            rng.choices(customer_ids, cum_weights=cum_weights)[0],
            rng.randint(1, n_products),
            rng.randint(1, 5),
            ts.isoformat(timespec="seconds"),
            rng.choice(STATUSES),
        ])
    header = ["order_id", "customer_id", "product_id", "quantity", "order_ts", "status"]
    return header, rows


def generate_events(rng, n_events, n_customers, n_products):
    # Newline-delimited JSON, the shape Spark reads with spark.read.json.
    start = datetime(2024, 1, 1)
    path = DATA_DIR / "web_events.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for _ in range(n_events):
            event = {
                "event_id": rng.getrandbits(48),
                "customer_id": rng.randint(1, n_customers),
                "product_id": rng.randint(1, n_products),
                "event_type": rng.choice(EVENT_TYPES),
                "event_ts": (start + timedelta(seconds=rng.randint(0, 90 * 24 * 3600))).isoformat(),
            }
            f.write(json.dumps(event) + "\n")
    print(f"  {path.relative_to(ROOT)}: {n_events:,} rows")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--customers", type=int, default=1_00)
    parser.add_argument("--products", type=int, default=1_00)
    parser.add_argument("--orders", type=int, default=1_00)
    parser.add_argument("--events", type=int, default=1_00)
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    rng = random.Random(SEED)
    print(f"Writing sample data to {DATA_DIR.relative_to(ROOT)}/")

    write_csv(DATA_DIR / "customers.csv", *generate_customers(rng, args.customers))
    write_csv(DATA_DIR / "products.csv", *generate_products(rng, args.products))
    write_csv(DATA_DIR / "orders.csv",
              *generate_orders(rng, args.orders, args.customers, args.products))
    generate_events(rng, args.events, args.customers, args.products)


if __name__ == "__main__":
    main()
