#!/usr/bin/env python3
"""One-shot seed of BATCH data (the 'raw' layer of the lakehouse).

Writes to /data/bronze:
  customers.json      10k customers
  products.json        500 products across 8 categories
  orders_raw/          ~60k orders in 6 part files (nested + messy on purpose)
  country_region.csv   small dimension table (CSV on purpose - mixed formats)

The messiness is intentional: dirty types, missing fields, duplicate ids and
an untyped JSON string column are exactly what bronze-layer pipelines must
handle before silver/gold.
"""
import json
import random
import sys
import time
from pathlib import Path

import numpy as np
from faker import Faker

BASE = Path("/data")
SEED = 20261012
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

CURRENCIES = ["EUR", "USD", "GBP", "INR"]
CHANNELS = ["web", "ios_app", "android_app", "pos", "partner"]
PAYMENT_METHODS = ["card", "paypal", "sepa", "apple_pay", "google_pay", "crypto"]
ORDER_STATUS = ["created", "paid", "shipped", "delivered", "cancelled", "refunded"]
CATEGORIES = ["electronics", "fashion", "home", "sports", "grocery", "toys", "books", "beauty"]
COUNTRIES = ["DE", "US", "GB", "IN", "FR", "ES", "IT", "BR", "JP", "AU"]
REGIONS = {
    "DE": "EMEA", "FR": "EMEA", "ES": "EMEA", "IT": "EMEA", "GB": "EMEA",
    "US": "NA", "BR": "LATAM", "IN": "APAC", "JP": "APAC", "AU": "APAC",
}

N_CUSTOMERS = 10_000
N_PRODUCTS = 500
N_ORDERS = 60_000
N_PARTS = 6
DAYS = 30


def make_customers():
    rows = []
    for i in range(N_CUSTOMERS):
        country = random.choice(COUNTRIES)
        rows.append({
            "customer_id": f"C{100000 + i}",
            "name": fake.name(),
            "email": fake.free_email(),
            "country": country,
            "city": fake.city(),
            "signup_date": fake.date_between(start_date="-4y", end_date="-30d").isoformat(),
            "loyalty_tier": random.choices(["bronze", "silver", "gold"], [0.6, 0.3, 0.1])[0],
            "is_active": random.random() > 0.15,
        })
    return rows


def make_products():
    rows = []
    for i in range(N_PRODUCTS):
        cat = random.choice(CATEGORIES)
        rows.append({
            "product_id": f"P{10000 + i}",
            "sku": f"SKU-{cat[:3].upper()}-{i:05d}",
            "name": f"{fake.word().title()} {fake.word().title()}",
            "category": cat,
            "price": round(random.lognormvariate(3.2, 0.9), 2),
            "cost": None,  # filled below
            "stock": random.randint(0, 500),
        })
    for r in rows:  # cost always below price
        r["cost"] = round(r["price"] * random.uniform(0.3, 0.75), 2)
    return rows, {r["product_id"]: r for r in rows}


def make_orders(product_prices):
    rows = []
    start_ts = time.time() - DAYS * 86400
    seen_ids = []
    for i in range(N_ORDERS):
        oid = f"O{900000 + i}" if random.random() > 0.02 else random.choice(seen_ids[-500:]) if seen_ids else f"O{900000 + i}"
        seen_ids.append(oid)
        pid = random.choice(list(product_prices.keys()))
        qty = random.choices([1, 2, 3, 4, 5], [50, 25, 12, 8, 5])[0]
        if random.random() < 0.04:
            qty = str(qty)  # dirty: numeric column arriving as string (ANSI trap)
        price = product_prices[pid]["price"]
        ts = start_ts + random.random() * DAYS * 86400
        order = {
            "order_id": oid,
            "order_ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)),
            "customer_id": f"C{100000 + random.randint(0, N_CUSTOMERS - 1)}",
            "product_id": pid,
            "quantity": qty,
            "unit_price": price,
            "total_amount": round((float(qty) if isinstance(qty, str) else qty) * price, 2),
            "currency": random.choices(CURRENCIES, [40, 30, 12, 18])[0],
            "channel": random.choices(CHANNELS, [45, 20, 15, 12, 8])[0],
            "payment_method": random.choices(PAYMENT_METHODS, [50, 20, 12, 8, 6, 4])[0],
            "status": random.choices(ORDER_STATUS, [8, 35, 25, 25, 5, 2])[0],
            "customer_note": fake.sentence() if random.random() < 0.1 else None,
            "items": [
                {"sku": f"SKU-{pid}", "qty": qty if isinstance(qty, int) else int(qty), "price": price}
                for _ in range(random.choice([1, 1, 1, 2]))
            ],
            "risk_flags": random.choices([[], ["new_device"], ["high_amount", "geo_mismatch"]], [90, 7, 3])[0],
            # Untyped JSON payload -> perfect for VARIANT parse_json() demos
            "vendor_meta": json.dumps({
                "vendor_id": f"V{random.randint(1, 40)}",
                "warehouse": random.choice(["WH-EU-1", "WH-US-2", "WH-APAC-1"]),
                "shipping": {
                    "carrier": random.choice(["dhl", "ups", "fedex", "dpd"]),
                    "eta_days": random.randint(1, 12),
                },
                "promo": {"code": None, "discount_pct": 0},
            } if random.random() < 0.9 else {
                "vendor_id": f"V{random.randint(1, 40)}",  # inconsistent keys across rows
                "partner": "xyz",
            }),
        }
        if random.random() < 0.25:  # sparse column -> schema evolution / merge-on-read demos
            order["loyalty_tier"] = random.choices(["bronze", "silver", "gold"], [6, 3, 1])[0]
        rows.append(order)
    random.shuffle(rows)
    return rows


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    bronze = BASE / "bronze"
    print(f"[seed] writing customers ...")
    write_jsonl(bronze / "customers.json", make_customers())

    print(f"[seed] writing products ...")
    products, prices = make_products()
    write_jsonl(bronze / "products.json", products)

    print(f"[seed] writing {N_ORDERS} orders into {N_PARTS} part files ...")
    orders = make_orders(prices)
    per = N_ORDERS // N_PARTS
    for p in range(N_PARTS):
        chunk = orders[p * per:(p + 1) * per] if p < N_PARTS - 1 else orders[p * per:]
        write_jsonl(bronze / "orders_raw" / f"part-{p:05d}.json", chunk)

    print(f"[seed] writing country_region.csv ...")
    csv_path = bronze / "country_region.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("country,region,region_tax_multiplier\n")
        for c in COUNTRIES:
            f.write(f"{c},{REGIONS[c]},{round(random.uniform(0.8, 1.3), 2)}\n")

    total = sum(f.stat().st_size for f in bronze.rglob("*") if f.is_file())
    print(f"[seed] done. {round(total / 1e6, 1)} MB written under {bronze}")


if __name__ == "__main__":
    sys.exit(main())
