"""Kafka example 4 - Admin API: create topic, describe, list consumer-group lag.
Run: python examples/kafka/04_admin_lag.py
"""
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:29092"
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

# 1) create a compacted topic (latest value per key is kept - e.g. reference data / changelog)
futures = admin.create_topics([NewTopic(
    "demo.users.compacted", num_partitions=3, replication_factor=1,
    config={"cleanup.policy": "compact", "min.cleanable.dirty.ratio": "0.1"},
)])
for topic, f in futures.items():
    try:
        f.result()
        print("created", topic)
    except Exception as e:  # already exists is fine for a demo
        print("skip", topic, "->", e)

# 2) lag = log-end-offset - committed offset, per partition
group, topic = "orders-processor", "orders.raw"
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
meta = c.list_topics(topic, timeout=10).topics[topic]
parts = [TopicPartition(topic, p) for p in meta.partitions]
committed = c.committed(parts, timeout=10)
total = 0
for tp in committed:
    _, high = c.get_watermark_offsets(tp, timeout=10)
    pos = tp.offset if tp.offset >= 0 else 0
    lag = high - pos
    total += lag
    print(f"partition {tp.partition}: committed={tp.offset} end={high} lag={lag}")
print("TOTAL LAG:", total)
c.close()
