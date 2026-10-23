"""Kafka example 3 - Exactly-once read-process-write with transactions.

consume(topic demo.events) -> transform -> produce(demo.events.out) + commit consumer offsets
all in ONE atomic transaction. Downstream consumers must use isolation.level=read_committed.
Run: python examples/kafka/03_transactions.py
"""
import json

from confluent_kafka import Consumer, KafkaException, Producer

BOOTSTRAP = "localhost:29092"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "eos-demo",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
    "isolation.level": "read_committed",   # never see aborted transactional writes
})
producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "transactional.id": "eos-demo-1",      # stable per logical instance -> zombie fencing
})
producer.init_transactions()
consumer.subscribe(["demo.events"])

try:
    while True:
        batch = consumer.consume(num_messages=100, timeout=1.0)
        if not batch:
            continue
        producer.begin_transaction()
        try:
            for m in batch:
                if m.error():
                    raise KafkaException(m.error())
                out = {"upper_key": (m.key() or b"").decode().upper(), "size": len(m.value())}
                producer.produce("demo.events.out", key=m.key(), value=json.dumps(out))
            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata(),
            )
            producer.commit_transaction()
        except Exception:
            producer.abort_transaction()   # outputs + offsets roll back together
            raise
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
