#!/usr/bin/env python3
"""Fake event-stream generator - writes newline-delimited JSON file 'parts'
into /data/bronze/events_stream/ every N seconds, like a landing zone would
receive from an upstream system (Kafka consumer, API dump, CDC export...).

Design notes (these matter for the streaming exercises):
  * Each file is written to a .tmp name and atomically renamed, so Spark's
    file source never sees a partial file (exactly what you want when a
    producer streams into a data lake directory).
  * --late-pct   : events get timestamps in the past -> watermarking demos
  * --dup-pct    : repeats a previously emitted event_id -> dropDuplicates demos
  * --events     : rows per file; combine with spark.readStream.maxFilesPerTrigger
"""
import argparse
import json
import os
import random
import time
import uuid
from pathlib import Path

from faker import Faker

OUT = Path("/data/bronze/events_stream")

EVENT_TYPES = ["page_view", "add_to_cart", "order_created", "payment_processed", "refund_issued"]
DEVICES = ["desktop", "mobile", "tablet", "pos"]
PAGES = ["/home", "/product", "/cart", "/checkout", "/account", "/search", "/deals"]

fake = Faker()


def make_event(rng: random.Random, ts: float):
    et = rng.choices(EVENT_TYPES, weights=[45, 25, 15, 10, 5])[0]
    ev = {
        "event_id": str(uuid.uuid4()),
        "event_type": et,
        "ts": ts,
        "session_id": f"S{rng.randint(1, 50000):06d}",
        "user_id": f"C{100000 + rng.randint(0, 9999)}",
        "device": rng.choice(DEVICES),
        "page": rng.choice(PAGES),
        "geo": {
            "country": rng.choice(["DE", "US", "GB", "IN", "FR"]),
            "city": fake.city(),
        },
        "properties": {
            "price": round(rng.uniform(5, 500), 2),
            "items": rng.randint(1, 6),
            "ab_test_group": rng.choice(["control", "variant_a"]),
        },
    }
    if et in ("order_created", "payment_processed", "refund_issued"):
        ev["order_id"] = f"O{900000 + rng.randint(0, 59999)}"
        ev["amount"] = round(rng.uniform(10, 1200), 2)
    return ev


def emit(path: Path, events):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    os.replace(tmp, path)  # atomic rename


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=3.0, help="seconds between files")
    ap.add_argument("--events", type=int, default=200, help="events per file")
    ap.add_argument("--late-pct", type=float, default=3.0, help="% of events are late (past ts)")
    ap.add_argument("--dup-pct", type=float, default=2.0, help="% of events duplicate an earlier id")
    ap.add_argument("--once", action="store_true", help="write a single file and exit")
    args = ap.parse_args()

    rng = random.Random(int(os.environ.get("GEN_SEED", "42")))
    OUT.mkdir(parents=True, exist_ok=True)
    print(f"[generator] streaming events -> {OUT} "
          f"(interval={args.interval}s, events={args.events}, late={args.late_pct}%, dup={args.dup_pct}%)",
          flush=True)

    history = []
    n_files = 0
    while True:
        ts = time.time()
        events = [make_event(rng, ts) for _ in range(args.events)]

        for e in events:
            if rng.random() < args.late_pct / 100:
                e["ts"] = ts - rng.uniform(60, 1800)  # late arrival
        if history and rng.random() < args.dup_pct / 100:
            events.append(dict(rng.choice(history)))  # exact duplicate event

        name = OUT / f"events_{int(ts * 1000):013d}_{uuid.uuid4().hex[:6]}.json"
        emit(name, events)
        history.extend(events)
        if len(history) > 5000:
            history = history[-2000:]
        n_files += 1
        if n_files % 20 == 0:
            print(f"[generator] {n_files} files written, latest={name.name}", flush=True)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
