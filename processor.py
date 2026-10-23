"""Stream processor: orders.raw -> validate -> enrich -> orders.enriched  (bad events -> orders.dlq).

Delivery semantics: AT-LEAST-ONCE (commit offsets only after output is flushed).
Duplicates are harmless downstream: the Pinot upsert table is idempotent per order_id.
For exactly-once, use Kafka transactions (see examples/kafka/03_transactions.py).
"""
from __future__ import annotations

import json
import logging
import signal

from confluent_kafka import Consumer, KafkaException, Producer

from app.common import (BOOTSTRAP, TOPIC_DLQ, TOPIC_ENRICHED, TOPIC_RAW, ValidationError, enrich_event)

log = logging.getLogger("processor")
running = True


def _stop(*_):
    global running
    running = False


def process_message(msg_value: bytes | None) -> tuple[str, bytes]:
    """Return (target_topic, payload). Pure-ish -> unit-testable without Kafka."""
    try:
        event = json.loads(msg_value) if msg_value else None
        return TOPIC_ENRICHED, json.dumps(enrich_event(event)).encode()
    except (ValidationError, json.JSONDecodeError, TypeError) as exc:
        dlq = {"error": str(exc), "payload": (msg_value or b"").decode("utf-8", "replace")}
        return TOPIC_DLQ, json.dumps(dlq).encode()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "orders-processor",
        "enable.auto.commit": False,          # we commit manually after produce succeeded
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
        "max.poll.interval.ms": 300000,
    })
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 20,
    })
    consumer.subscribe([TOPIC_RAW])
    ok = bad = 0
    try:
        while running:
            batch = consumer.consume(num_messages=500, timeout=1.0)
            if not batch:
                continue
            for msg in batch:
                if msg.error():
                    raise KafkaException(msg.error())
                topic, payload = process_message(msg.value())
                producer.produce(topic, key=msg.key(), value=payload)
                if topic == TOPIC_DLQ:
                    bad += 1
                else:
                    ok += 1
            producer.flush(30)        # make sure output is durable ...
            consumer.commit(asynchronous=False)  # ... then commit input offsets
            log.info("ok=%d dlq=%d", ok, bad)
    finally:
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    main()
