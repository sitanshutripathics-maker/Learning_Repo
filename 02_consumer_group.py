"""Kafka example 2 - Consumer in a consumer group with manual commits and graceful shutdown.

Start 2-3 copies of this script in different terminals to watch partitions being
rebalanced between members of the same group.
Run: python examples/kafka/02_consumer_group.py [group-id]
"""
import sys

from confluent_kafka import Consumer, KafkaException

group = sys.argv[1] if len(sys.argv) > 1 else "demo-group"
consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": group,
    "auto.offset.reset": "earliest",   # only used when the group has no committed offset
    "enable.auto.commit": False,       # commit AFTER processing -> at-least-once
    "partition.assignment.strategy": "cooperative-sticky",
})


def on_assign(c, partitions):
    print("assigned:", [(p.topic, p.partition) for p in partitions])


def on_revoke(c, partitions):
    print("revoked :", [(p.topic, p.partition) for p in partitions])


consumer.subscribe(["demo.events"], on_assign=on_assign, on_revoke=on_revoke)
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        print(f"[{msg.partition()}@{msg.offset()}] {msg.key()} -> {msg.value()[:80]}")
        consumer.commit(message=msg, asynchronous=False)  # commit offset+1 of this message
except KeyboardInterrupt:
    pass
finally:
    consumer.close()  # leaves the group cleanly -> immediate rebalance, committed offsets kept
