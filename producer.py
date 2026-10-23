"""Simulated order service: emits order lifecycle events to Kafka (topic orders.raw).

Key = order_id  -> all events of an order land in the same partition -> ordered.
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid

from confluent_kafka import Producer

from app.common import (BOOTSTRAP, CATALOG, CITIES, PAYMENT_METHODS, TOPIC_RAW, new_event_id)

log = logging.getLogger("producer")

# Probability that an order moves on to the next state after each step.
LIFECYCLE = [("CREATED", 1.0), ("PAID", 0.92), ("SHIPPED", 0.85), ("DELIVERED", 0.9)]


def build_order_events(now_ms: int, rng: random.Random, bad_ratio: float = 0.01) -> list[dict]:
    """Create the (future-dated) event sequence of one order. Pure function -> testable."""
    order = {
        "order_id": f"O-{uuid.UUID(int=rng.getrandbits(128)).hex[:12]}",
        "user_id": f"U{rng.randint(1, 5000):05d}",
        "product_id": rng.choice(list(CATALOG)),
        "quantity": rng.randint(1, 4),
        "payment_method": rng.choice(PAYMENT_METHODS),
        "city": rng.choice(CITIES),
    }
    events, t = [], now_ms
    for status, p in LIFECYCLE:
        if rng.random() > p:
            if status != "CREATED":
                events.append({**order, "event_id": new_event_id(), "status": "CANCELLED", "event_time": t})
            break
        events.append({**order, "event_id": new_event_id(), "status": status, "event_time": t})
        t += rng.randint(1_000, 5_000)
    if rng.random() < bad_ratio:  # poison pill to exercise the DLQ path
        events[-1] = {**events[-1], "quantity": -1}
    return events


def delivery_report(err, msg):
    if err is not None:
        log.error("delivery failed: %s", err)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=20, help="new orders per second")
    ap.add_argument("--duration", type=int, default=0, help="seconds to run (0 = forever)")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    rng = random.Random(args.seed)
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,       # no duplicates / reordering on retry
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 20,                  # small batching window -> throughput
        "batch.size": 64 * 1024,
    })

    pending: list[tuple[float, dict]] = []  # (due_wallclock_seconds, event)
    start, sent = time.time(), 0
    next_order = last_log = start
    try:
        while args.duration == 0 or time.time() - start < args.duration:
            now = time.time()
            while next_order <= now:
                for ev in build_order_events(int(now * 1000), rng):
                    pending.append((ev["event_time"] / 1000.0, ev))
                next_order += 1.0 / args.rate
            due = [p for p in pending if p[0] <= now]
            pending = [p for p in pending if p[0] > now]
            for _, ev in due:
                producer.produce(TOPIC_RAW, key=ev["order_id"], value=json.dumps(ev), on_delivery=delivery_report)
                sent += 1
            producer.poll(0)
            if now - last_log >= 5:
                last_log = now
                log.info("sent=%d pending=%d", sent, len(pending))
            time.sleep(0.05)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)
        log.info("done, total sent=%d", sent)


if __name__ == "__main__":
    main()
