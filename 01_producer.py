"""Kafka example 1 - Producer: keys, partitions, acks, idempotence, delivery callbacks.

Prereq: project stack running (docker compose up -d) -> broker at localhost:29092
Run:    python examples/kafka/01_producer.py
"""
import json
import time

from confluent_kafka import Producer

BOOTSTRAP = "localhost:29092"
TOPIC = "demo.events"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",                 # wait for all in-sync replicas -> durability
    "enable.idempotence": True,    # broker de-duplicates retries, keeps per-partition order
    "compression.type": "lz4",
    "linger.ms": 10,               # wait up to 10ms to fill a batch (throughput vs latency)
})


def on_delivery(err, msg):
    # Called from producer.poll()/flush(): ack or error per message
    if err:
        print(f"FAILED: {err}")
    else:
        print(f"ok -> {msg.topic()}[{msg.partition()}] @ offset {msg.offset()} key={msg.key().decode()}")


for i in range(10):
    order_id = f"demo-{i % 3}"      # same key -> same partition -> ordered
    event = {"order_id": order_id, "seq": i, "ts": int(time.time() * 1000)}
    producer.produce(TOPIC, key=order_id, value=json.dumps(event), on_delivery=on_delivery)
    producer.poll(0)               # serve delivery callbacks

producer.flush()                   # block until everything is acknowledged
