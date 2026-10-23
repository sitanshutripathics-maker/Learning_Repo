# 05 — High-Level Interview Questions & Answers (Kafka + Pinot)

How to use: cover the answer, say yours aloud, compare. ⭐ = asked very frequently. Levels: 🟢 fundamentals · 🟡 intermediate/senior · 🔴 architect/staff.

## Contents
- [Part A — Kafka](#part-a--kafka)
- [Part B — Apache Pinot](#part-b--apache-pinot)
- [Part C — Kafka + Pinot together / system design](#part-c--kafka--pinot-together--system-design)
- [Part D — Scenario & troubleshooting](#part-d--scenario--troubleshooting)
- [Part E — Coding / hands-on prompts](#part-e--coding--hands-on-prompts)
- [Part F — Behavioural / project questions](#part-f--behavioural--project-questions)
- [Quick revision sheet](#quick-revision-sheet)

---

# Part A — Kafka

## A1. Fundamentals 🟢

**1. ⭐ What is Kafka and what problems does it solve?**
A distributed, replicated, partitioned commit log / event-streaming platform. It decouples producers and consumers, buffers bursts (back-pressure), provides durable ordered event storage with replay, and scales horizontally. Typical uses: messaging, activity tracking, log aggregation, CDC, stream processing, event sourcing, feeding analytics stores (Pinot).

**2. ⭐ Kafka vs a traditional message queue (RabbitMQ/SQS)?**
Kafka: pull-based, log retained regardless of consumption, replay by offset, multiple independent consumer groups, partition-level ordering, very high throughput. Traditional MQ: usually push, message deleted on ack, rich routing/priorities/per-message TTL, lower throughput at scale. Pick Kafka for streams & replay; RabbitMQ/SQS for task queues & complex routing.

**3. Explain topic, partition, offset, broker.**
Topic = named stream; split into partitions (ordered immutable logs); every record has an offset unique within its partition; brokers store partitions; each partition has a leader and followers.

**4. ⭐ How does Kafka guarantee ordering?**
Only within a partition. Same key → same partition (hash(key) % partitions) → ordered. No global ordering across partitions. To preserve order with retries use idempotent producer (or `max.in.flight ≤ 5` with idempotence).

**5. ⭐ What is a consumer group? What happens if consumers > partitions?**
A set of consumers sharing a `group.id`; each partition is assigned to exactly one member, so the group collectively consumes each record once. Extra consumers (> partitions) are idle. Different groups each receive all records.

**6. What is an offset and where is it stored?**
Position of a record in a partition. Consumer's committed offset (next to read) is stored in the internal compacted topic `__consumer_offsets` (50 partitions by default). Consumers may also store offsets externally (DB) for transactional processing.

**7. What does ZooKeeper do, and what is KRaft?**
Old Kafka used ZooKeeper for metadata/controller election. KRaft replaces it with a Raft-based controller quorum storing metadata in the `__cluster_metadata` log — fewer moving parts, faster failover, higher partition limits. ZooKeeper removed in Kafka 4.0.

**8. What are Kafka's guarantees?**
Per-partition ordering; committed (acked by all ISR) records are not lost as long as at least one in-sync replica survives; consumers see records in order, each at least once by default (exactly-once with transactions).

**9. How is data stored on disk?**
Per-partition directory with segment files: `.log` (records), `.index` (offset→position, sparse), `.timeindex`. Appends are sequential; retention deletes/compacts whole segments.

**10. Why is Kafka fast?**
Sequential I/O, page cache, zero-copy sendfile, batching + compression, partition parallelism, binary protocol, no per-message broker bookkeeping (consumers track position).

## A2. Producers & consumers 🟡

**11. ⭐ Explain `acks` 0/1/all and `min.insync.replicas`.**
0: don't wait (may lose). 1: leader wrote it (lost if leader dies before replication). all: all ISR have it. With RF=3, `min.insync.replicas=2`, `acks=all` → tolerate one broker loss without losing acknowledged data; if ISR < 2 produce fails (`NotEnoughReplicas`) — consistency over availability.

**12. ⭐ What is an idempotent producer? How is it different from transactions?**
Broker assigns a Producer ID; each batch has a sequence number per partition; broker drops duplicates and detects gaps → no duplicates/reordering caused by retries **within a single producer session and partition**. Transactions add atomic multi-partition writes + offset commit and cross-session fencing via `transactional.id`.

**13. ⭐ How do you achieve exactly-once?**
Within Kafka: idempotent producer + transactions (`begin → produce → send_offsets_to_transaction → commit`) and `read_committed` consumers; Kafka Streams `exactly_once_v2`. To external systems: idempotent/upsert writes, unique keys, transactional outbox, or two-phase patterns. "Exactly-once" = at-least-once delivery + idempotent processing/atomic commit.

**13a. Why might a transactional consumer still see duplicates?**
Consumers not using `read_committed` see aborted records; side effects outside Kafka aren't rolled back; reading from a non-transactional source.

**14. At-most-once vs at-least-once vs exactly-once — how do commits relate?**
Commit before processing → at-most-once; process then commit → at-least-once; atomic (output + offsets) → exactly-once.

**15. ⭐ What triggers a rebalance and how do you minimize it?**
Member join/leave/crash, missed heartbeats (`session.timeout.ms`), exceeding `max.poll.interval.ms`, partition count change, subscription change. Mitigate: cooperative-sticky assignor, static membership (`group.instance.id`), tune timeouts, keep poll loop fast (offload slow work / reduce `max.poll.records`), graceful shutdown, avoid autoscaling flapping.

**16. Difference between `session.timeout.ms`, `heartbeat.interval.ms`, `max.poll.interval.ms`?**
Heartbeats run in a background thread: if none within session timeout → member dead. `max.poll.interval.ms` ensures the *application* is making progress (time between `poll()`s) — exceeding it → member evicted though heartbeats continue.

**17. How do you handle a poison message?**
Catch deserialization/processing errors → send to DLQ with error metadata (topic/partition/offset/exception), commit, continue. For transient errors use retry topics with back-off (`retry-5s`, `retry-1m`) then DLQ. Never loop infinitely blocking the partition.

**18. How do you choose the partition key?**
High cardinality, evenly distributed, and aligned with ordering/co-location needs (e.g. `order_id`, `user_id`). Avoid skewed keys (e.g. country); if you need ordering per entity key by entity. Keys are immutable decisions — repartitioning later breaks locality.

**19. How many partitions should a topic have?**
`max(target_throughput / producer_per_partition, target_throughput / consumer_per_partition)` and ≥ desired max consumer parallelism, with headroom (can only increase). Too many → more file handles, longer leader election, memory, end-to-end latency, rebalance cost.

**20. Can you decrease the number of partitions?** No — create a new topic and migrate. Increasing breaks key→partition mapping for existing keys.

**21. What is consumer lag and how do you reduce it?**
`log-end-offset − committed offset`. Reduce: more consumers (≤ partitions), more partitions, optimize processing, batch/async I/O, increase `fetch`/`max.poll.records`, scale downstream sink, check rebalances / GC.

**22. Push vs pull — why does Kafka use pull?**
Consumers control rate (back-pressure), can batch efficiently, replay and re-read; broker stays simple. Long-polling (`fetch.max.wait.ms`) avoids busy-waiting.

**23. What is `auto.offset.reset`?**
Behaviour when no committed offset (or offset out of range): `earliest`, `latest`, `none` (throw). It does NOT apply if offsets exist.

**24. How to reprocess/replay data?**
Stop consumers, reset group offsets (`kafka-consumer-groups --reset-offsets --to-datetime/--to-earliest --execute`), or use a new `group.id`, or `seek()` programmatically. Requires data within retention.

**25. Producer buffering/backpressure: what if the buffer is full?**
`buffer.memory` exhausted → `send()` blocks up to `max.block.ms` then throws. Handle by slowing production, increasing broker capacity/batching, or shedding load.

## A3. Brokers, replication, storage 🟡

**26. ⭐ Explain ISR, leader election, high watermark.**
ISR = replicas caught up within `replica.lag.time.max.ms`. Leader serves reads/writes; if it fails the controller elects a new leader from ISR. High watermark = highest offset replicated to all ISR; consumers can only read up to it. Epochs (leader epoch) prevent log divergence after elections.

**27. What is unclean leader election?** Electing an out-of-sync replica when no ISR members are alive: availability up, data loss possible. Default false; keep false for critical data.

**28. What happens when a broker dies?** Controller detects (KRaft heartbeat), elects new leaders from ISR for affected partitions, clients refresh metadata and continue; under-replicated partitions recover when the broker returns (it truncates to high watermark and catches up).

**29. ⭐ Retention vs compaction?** Delete policy removes old segments by time/size. Compaction keeps the latest record per key; tombstone (null value) deletes a key. Use compaction for state/changelog/reference data, delete for event streams. Can combine (`compact,delete`).

**30. How do you size a Kafka cluster?**
Estimate ingress MB/s, fan-out (consumer groups), retention, RF: storage = ingress × retention × RF; network = ingress × (RF − 1 + consumers); partitions per broker (< ~4000 classic guidance); CPU for TLS/compression; plan 60-70 % utilization headroom; benchmark with `kafka-producer-perf-test`.

**31. How do you do a zero-downtime rolling upgrade?**
Upgrade one broker at a time, wait until under-replicated partitions = 0, keep `inter.broker.protocol.version` (or `metadata.version` in KRaft) pinned until all are upgraded, then bump. Controlled shutdown + preferred leader election.

**32. What are quotas?** Per-client/user byte-rate and request-rate throttles to protect the cluster from noisy neighbours.

**33. What is rack awareness?** `broker.rack` + `replica.selector.class` — replicas spread over AZs; follower fetching lets consumers read from nearest replica (KIP-392), cutting cross-AZ cost.

**34. What is tiered storage?** (KIP-405) offload older log segments to object storage, keeping only hot data on local disks — cheaper long retention.

**35. How does Kafka handle large messages?** Default `message.max.bytes` ~1 MB. Prefer claim-check (store blob in S3, send reference). Or raise `max.message.bytes`, `replica.fetch.max.bytes`, `max.request.size`, `fetch.max.bytes` consistently and use compression.

## A4. Ecosystem & patterns 🟡/🔴

**36. Kafka Connect: what and why?** Framework for scalable, fault-tolerant, config-driven integration (source/sink connectors, converters, SMTs, distributed workers with offset/config/status topics). Avoids hand-written ingestion code. Example: Debezium MySQL/Postgres CDC → Kafka → S3/Elasticsearch/Pinot.

**37. Kafka Streams vs Flink vs Spark Structured Streaming?**
Kafka Streams: a Java library (no cluster), tied to Kafka, state in RocksDB + changelog topics, great for microservices. Flink: full distributed engine, true event-time, large state, many sources/sinks, SQL. Spark SS: micro-batch (and continuous), unified batch/stream, good with lakehouse.

**38. What is a KTable / state store / changelog?** KTable = changelog stream viewed as table (latest per key). State stores (RocksDB) hold local state; changes are logged to compacted changelog topics for recovery; standby replicas speed failover.

**39. Schema Registry & compatibility?** Central schema store with ids embedded in messages; compatibility checks prevent breaking consumers. BACKWARD = new schema reads old data (default); FORWARD = old reads new; FULL both. Evolve by adding optional fields with defaults.

**40. Avro vs Protobuf vs JSON?** JSON easy/human readable/no schema/large; Avro compact, schema in registry, great evolution, popular in data platforms; Protobuf compact, strong codegen, popular in microservices; all supported by Schema Registry.

**41. ⭐ Transactional outbox / dual-write problem?** Writing to DB and Kafka separately can diverge. Write the event to an `outbox` table in the same DB transaction; a CDC connector (Debezium) publishes it to Kafka → atomic & reliable.

**42. CDC with Debezium — how does it work?** Reads the DB's transaction log (binlog/WAL), emits change events (before/after, op, ts) per table topic; supports snapshots, schema history, tombstones, outbox event router.

**43. Event sourcing & CQRS with Kafka?** Store state changes as an immutable ordered log (source of truth); build read models (Pinot/ES/DB) by consuming it. Replay rebuilds any view. Challenges: schema evolution, snapshots, GDPR deletion (crypto-shredding, compaction).

**44. Multi-DC / DR strategies?** MirrorMaker 2 / Cluster Linking (active-passive or active-active), stretch cluster across 3 AZs (sync, RF=3, rack-aware) for single region HA; consumer offset translation; plan RPO/RTO.

**45. How do you secure Kafka?** TLS (encryption in transit), SASL (SCRAM/OAUTHBEARER/Kerberos) or mTLS for authn, ACLs/RBAC for authz, encrypt sensitive fields, audit logs, network segmentation, secret management, disable plaintext listeners.

**46. How do you monitor Kafka?** JMX→Prometheus→Grafana, Kafka UI, Burrow/Kafka Lag Exporter, Cruise Control. Key: consumer lag, URP, offline partitions, active controller count, request latency/queue times, ISR changes, disk/NW, GC, producer error/retry rates.

**47. Common Kafka performance tuning?**
Producer: `batch.size`↑, `linger.ms`↑, compression (lz4/zstd), `acks`, buffer. Consumer: `fetch.min.bytes`, `max.poll.records`, parallelism. Broker: `num.network.threads`, `num.io.threads`, disks (JBOD/XFS), page cache sizing, avoid swapping, tuned retention/segment sizes.

## A5. Scenario/deep dive 🔴

**48. Design a system guaranteeing no loss and no duplicates from DB → Kafka → data store.**
Outbox + Debezium (at-least-once) → Kafka (RF=3, min ISR=2, idempotent producer) → consumer with idempotent sink (upsert by event id/PK) or transactional (Kafka→Kafka EOS) → verify with reconciliation jobs/counters.

**49. Per-key ordering with parallel processing in one consumer?** Partition-level parallelism only. Within a consumer, process per-key sequentially: hash keys to worker threads/queues, commit only the contiguous lowest completed offset per partition.

**50. How do you handle backpressure in consumers?** `pause()/resume()` partitions, bounded in-process queues, tune `max.poll.records`, autoscale on lag (KEDA), shed/queue to DLQ, scale sink.

**51. What are the downsides of too many partitions?** More open files & memory, longer controller failover/recovery, higher end-to-end latency due to replication fan-out, increased rebalance time, more client metadata.

**52. What happens if the consumer crashes after processing but before committing?** On restart it re-reads uncommitted records → duplicates; hence idempotent processing (dedup table, upserts, natural keys).

**53. How can you guarantee ordering during retries?** `enable.idempotence=true` (implies `acks=all`, retries, max.in.flight ≤ 5). For retry topics, ordering per key is lost unless you block the key/partition.

**54. How does a Kafka transaction work internally?** Transaction coordinator (a broker) + `__transaction_state`; producer registers partitions, writes records marked with PID/epoch, `commit` writes commit markers to every partition; `read_committed` consumers buffer until LSO (last stable offset) passes the marker. Epoch bump fences zombies.

**55. What is the Last Stable Offset (LSO)?** Offset below which all transactions are decided; `read_committed` consumers read up to the LSO; a hung transaction blocks them (`transaction.timeout.ms`).

---

# Part B — Apache Pinot

## B1. Fundamentals 🟢

**56. ⭐ What is Apache Pinot and why use it?**
Distributed real-time OLAP store optimized for low-latency, high-QPS analytical queries on fresh data (user-facing analytics). Ingests from Kafka in real time + batch from lakes, columnar segments, rich indexes (star-tree, inverted, range, bloom, text, JSON, geo), SQL.

**57. ⭐ Describe Pinot's architecture.**
Controller (Helix controller, metadata, segment assignment, admin REST/UI), Broker (routing, scatter-gather-merge), Server (stores/serves segments, consumes streams), Minion (background tasks), ZooKeeper (cluster state), deep store (durable segments). Tenants give isolation.

**58. What is a segment?** Immutable (when committed) columnar unit holding a slice of table rows with dictionaries, forward and optional indexes + metadata; unit of replication, assignment, pruning, parallel execution.

**59. ⭐ Schema vs table config?** Schema defines columns (dimension, metric, date-time, primary key). Table config defines type (REALTIME/OFFLINE), ingestion (stream or batch), indexes, routing, retention, upsert/dedup, tenants, quotas, tiers.

**60. Dimension vs metric vs time column?** Dimension: group-by/filter attributes; metric: numeric measures aggregated; time column: used for retention, pruning and hybrid boundary.

**61. ⭐ Real-time vs offline vs hybrid tables?** Realtime: streaming ingestion (Kafka). Offline: batch-built segments pushed in. Hybrid: same name both types; broker splits queries at the time boundary — realtime for recent, offline for historical (cleaned/compacted).

**62. How does a query execute in Pinot?** Broker parses/plans → prunes segments (time/partition/bloom) → routing table selects server per segment → servers run per-segment plans using indexes in parallel → broker merges partial aggregates → response with stats.

**63. Which SQL does Pinot support?** ANSI-ish SQL via Calcite: select/filter/group-by/having/order/limit, many UDFs; multi-stage engine adds joins, window functions, subqueries, set operations. No DML except via ingestion.

**64. ⭐ Default LIMIT?** 10 for selection queries if not specified. Always set explicit limits.

## B2. Ingestion 🟡

**65. ⭐ How does real-time ingestion from Kafka work?**
Each server runs low-level consumers, one per Kafka partition (per replica), writing into a mutable CONSUMING segment (immediately queryable). On threshold (rows/time/size) the segment is committed: built into immutable form with indexes, uploaded to deep store, ZK updated, new consuming segment starts at the next offset. Offsets are tracked in segment metadata, not Kafka consumer groups.

**66. What flush thresholds exist and how do you tune them?**
`flush.threshold.rows`, `.time`, `.segment.size`. Set rows=0 and segment.size (e.g. 200M) for auto-tuning to hit target segment size; time threshold bounds low-traffic partitions. Goal: segments 100-500 MB; avoid many tiny segments.

**67. What if Pinot server dies during consumption?** Other replicas continue; on restart the server re-consumes from the last committed offset (from segment metadata) – no data loss as long as Kafka retention covers it.

**68. How do you backfill/correct data?** Build offline segments (Spark/standalone job) and push to offline table; in hybrid tables the offline data supersedes realtime for the time range after the boundary moves (via RealtimeToOfflineSegmentsTask or manual push). For upsert tables, replay corrected events with newer comparison values.

**69. What decoders/formats are supported?** JSON, Avro (+Confluent Schema Registry), Protobuf, Thrift, CSV, Parquet (batch), custom decoder plugin.

**70. Ingestion-time transformations?** `transformConfigs` (Groovy/built-in functions), `filterConfig`, flattening JSON with `jsonPathString`, complex-type handling, null handling, derived time columns.

**71. How to handle schema evolution?** Add columns: update schema → reload segments (defaults for old data). Type change/rename: new column & backfill or rebuild. Producers must remain compatible (new Kafka fields ignored until schema updated).

**72. How does Pinot handle late/out-of-order data?** Append tables accept any time (goes into current consuming segment; time pruning uses segment min/max). Upsert tables use comparison column to ignore older versions (`dropOutOfOrderRecord`, `outOfOrderRecordColumn`).

## B3. Indexing & performance 🟡

**73. ⭐ List Pinot indexes and when to use each.**
Sorted (best filter on 1 col), inverted (equality/IN), range (inequalities), bloom (point lookups on high-cardinality, segment pruning), star-tree (pre-aggregation for fixed dashboard queries), text/FST (search/regex), JSON (nested filtering), geo/H3, timestamp, vector, plus dictionary vs raw forward index choice.

**74. ⭐ What is a star-tree index? Trade-offs?**
Pre-aggregated tree over a configured dimension split order with function-column pairs; queries touching those dims/aggregations read ≤ `maxLeafRecords` records instead of scanning all rows → bounded latency. Costs: storage + build time, only matching query shapes, only on completed segments, not for upsert tables, requires choosing the dimension split order carefully (typically highest-cardinality/most-filtered dimensions first) and verifying with the response stats.

**75. How do you diagnose a slow query?**
Check response stats: `numEntriesScannedInFilter` high → add index; `numSegmentsQueried` ≫ matched → missing time/partition pruning; large `numDocsScanned` with group-by → star-tree/pre-agg; `EXPLAIN PLAN FOR`; broker/server logs, `timeUsedMs` split (server vs broker reduce), segment count/size, hot servers (skew), GC, off-heap/page cache pressure.

**76. How do you scale reads (QPS) vs data volume?**
QPS: add replicas / replica groups (each query touches one group) and brokers; reduce per-query work (indexes, star-tree, caching). Data: add servers (+rebalance), tiering, segment compaction, retention.

**77. Dictionary-encoded vs raw forward index?** Dictionary: smaller for low/medium cardinality, enables inverted/range index and fast filter. Raw: for high-cardinality or large values (no dictionary lookup overhead; compression chunking).

**78. How do approximate aggregations help?** `DISTINCTCOUNTHLL`, `PERCENTILETDIGEST`, theta sketches give bounded memory and fast results; exact `DISTINCTCOUNT` requires shipping sets.

**79. What is segment pruning?** Broker/server skips segments using time ranges, partition metadata, bloom filters, min/max values → fewer segments scanned. Always filter on time column.

**80. Memory model of a Pinot server?** Completed segments are mmap'ed (off-heap, page cache); consuming segments and upsert key maps use heap/off-heap; leave RAM for OS page cache; avoid very large heaps; monitor direct memory.

## B4. Upsert & dedup 🟡/🔴

**81. ⭐ How does upsert work in Pinot, and what are the requirements?**
Server keeps primary-key → record location map + validDocIds bitmaps; newer comparison value invalidates older doc. Requires: `primaryKeyColumns` in schema, `upsertConfig` (FULL/PARTIAL), stream partitioned by primary key (same key → same partition), `strictReplicaGroup` routing. Queries are automatically filtered to valid docs.

**82. Full vs partial upsert?** FULL replaces the entire row; PARTIAL merges columns via strategies (OVERWRITE, INCREMENT, APPEND, UNION, MAX, MIN, IGNORE) — useful when events carry only changed fields.

**83. How do you delete in upsert tables?** `deleteRecordColumn` (boolean) soft-delete records; optionally TTL (`deletedKeysTTL`) to clean keys from the map; queries exclude deleted rows by default.

**84. Upsert drawbacks?** Memory for the key map, restart cost (mitigate with snapshots/preload), partitioning constraint, no star-tree, more complex rebalancing.

**85. Dedup vs upsert?** Dedup = drop later records with an already-seen primary key (first wins); upsert = keep latest (last wins by comparison column).

## B5. Operations 🟡/🔴

**86. How do you scale/rebalance a Pinot cluster?** Add servers (with tags), call rebalance API (`reassignInstances`, `minAvailableReplicas` for no downtime), verify ideal state vs external view; for brokers update broker tags.

**87. What is ideal state vs external view?** Ideal state: desired segment→server+state mapping in ZK (Helix). External view: actual state reported by instances. Mismatch indicates problems (segments in ERROR).

**88. What is the role of the controller in real-time segment completion?** Runs the segment completion FSM: servers report `segmentConsumed`; controller picks a committer (HOLD/CATCH_UP/COMMIT/DISCARD/KEEP), accepts upload, updates metadata, creates next consuming segment.

**89. What is Minion used for?** Distributed background tasks: merge/rollup of small segments, realtime→offline conversion, purge (GDPR), upsert compaction, segment refresh/generation.

**90. How do you do retention and tiering?** `retentionTimeValue/Unit` deletes old segments; `tierConfigs` moves segments older than X to cold server tags (cheaper disks); deep store retains for recovery.

**91. How do you secure Pinot?** TLS for controller/broker/server, Basic/OIDC auth, ACLs per table, access control factory, network isolation, secrets via env/vault, audit logs; separate admin vs query endpoints.

**92. Monitoring Pinot?** JMX → Prometheus/Grafana: broker query latency/exceptions/QPS, server `realtimeIngestionDelayMs`, consuming lag, segment count/size, ZK latency, heap/direct memory, controller `percentSegmentsAvailable`, `numMissingSegments`.

**93. What happens if ZooKeeper is down?** Existing queries still run with cached routing for a while, but segment commits, table changes, rebalances, and new consuming segments fail; run a 3-node ensemble and monitor it.

**94. How do you upgrade Pinot?** Rolling: ZK → controllers → brokers → servers → minions (order per docs), check compatibility notes and `useMultistageEngine` changes; test configs in staging.

## B6. Comparisons 🟡/🔴

**95. ⭐ Pinot vs Druid vs ClickHouse?** Pinot: strongest for high-QPS user-facing, built-in upsert, star-tree, rich indexes, multi-stage joins; ops-heavy. Druid: similar real-time + roll-up at ingest, mature time-series, limited joins. ClickHouse: simpler ops, very fast scans, strong SQL/joins, MergeTree; real-time freshness via inserts, upserts eventual (ReplacingMergeTree), concurrency tuning needed.

**96. Pinot vs Elasticsearch?** ES: search-first (inverted index, relevance), aggregations heavier/slower at scale; Pinot: analytics-first columnar, aggregation-optimized, cheaper storage; Pinot has text index for light search.

**97. Pinot vs Snowflake/BigQuery/Trino?** Warehouses/engines: elastic, complex SQL, seconds latency, low concurrency, storage-decoupled. Pinot: millisecond latency at thousands of QPS on hot, fresh data; typically complements the warehouse (serving layer).

**98. When would you NOT use Pinot?** Heavy ad-hoc joins across many large tables, OLTP/transactions, small data (Postgres suffices), full-text search primary use case, infrequent queries where warehouse cost is lower.

---

# Part C — Kafka + Pinot together / system design

**99. ⭐ Design a real-time analytics platform for ride-hailing/e-commerce.**
Sources → Kafka (keyed by entity id, Avro + Schema Registry, RF=3) → stream processing (Flink/Kafka Streams: validate, enrich, dedupe, sessionize) → enriched topics → Pinot realtime tables (append for events, upsert for current state) + S3 sink for lake/batch → hybrid with offline segments for history → API layer (caching, auth, rate limit) → dashboards/apps. Cover: partitioning, ordering, exactly-once/idempotency, DLQ, schema evolution, SLAs (freshness/latency), capacity, multi-AZ HA, monitoring, cost. (This repo's project is a scaled-down version.)

**100. ⭐ How do you ensure exactly-once (or effectively-once) from Kafka to Pinot?**
Pinot consumes with its own offset tracking per segment; segment commit is atomic w.r.t. offsets. Duplicates arise only from upstream (producer retries/processor reprocessing). Handle via idempotent/transactional upstream, **upsert** (last-wins) or **dedup** (first-wins) tables keyed by event/entity id.

**101. Why must the Kafka topic be partitioned by the Pinot primary key?**
Upsert state is local per server/partition. If versions of one key land in different partitions, servers can't see each other's keys → duplicates/stale rows. So `key = primary key` (same hash function as the table's partition config).

**102. What if you must change the number of Kafka partitions of an upsert source?**
Mapping changes → same key may now go to a different partition → old rows linger. Create a new topic & table (or re-bootstrap/backfill), cut over with dual-run. Plan partitions up front.

**103. How do you reconcile Pinot with the source of truth?** Periodic count/sum comparisons per time bucket vs warehouse/Kafka offsets; checksum jobs; alerts on ingestion delay; replay by resetting Pinot (re-create table with `smallest`) when retention allows.

**104. How does data freshness break down end-to-end?** Producer linger + broker replication + processor latency + Pinot consume (+ index build in consuming segment) ≈ sub-second to few seconds. Monitor `now − max(event_time)` in Pinot and Kafka lag.

**105. How do you handle schema changes end-to-end?** Schema Registry compat rules → add optional field in producers → update Pinot schema (add column) + reload → then consumers/APIs use it. Never rename/remove in place; use versioned topics for breaking changes.

**106. Multi-tenant / multi-region design?** Per-tenant topics or key prefix + Pinot tenants/tables; regional Kafka clusters with MirrorMaker; regional Pinot clusters consuming local topics; global view via aggregation tier or replicating curated topics; define RPO/RTO.

**107. How would you add real-time anomaly detection?** Stream processor computes windowed aggregates and emits alerts, or scheduled Pinot queries (ThirdEye) compare windows; alerts to PagerDuty; keep thresholds in config.

**108. Cost optimization levers?** Kafka: compression, retention, tiered storage, right-size partitions. Pinot: raw vs dictionary, drop unused indexes, rollup/merge, tiering to cheaper nodes, retention, shared tenants, spot instances for minions.

**109. Lambda vs Kappa architecture and where Pinot fits?** Lambda: batch + speed layers (Pinot hybrid table is a built-in form). Kappa: one stream pipeline with replay from Kafka (Pinot upsert/replay). Choose by reprocessing needs and retention.

**110. How do you secure the entire pipeline?** TLS everywhere, SASL/OIDC, per-service identities, ACLs (Kafka), table ACLs (Pinot), PII masking/tokenization in the processor, secrets in vault, audit, least privilege network policies, input validation in the API (parameter whitelist — Pinot has no bind variables).

---

# Part D — Scenario & troubleshooting

| # | Scenario | Likely causes → Actions |
|---|---|---|
| 111 | ⭐ **Consumer lag is increasing** | Slow processing, GC pauses, rebalance storms, insufficient consumers (≤ partitions), hot partition, downstream outage. Check per-partition lag; scale consumers/partitions; profile processing; batch writes; check `max.poll.interval`; pause/resume or DLQ failing messages. |
| 112 | ⭐ **Frequent rebalances** | Slow `poll` loop, short session timeout, pods crash-looping/autoscaling, network blips. Use cooperative-sticky, static membership, tune timeouts, fix crashes, graceful shutdown. |
| 113 | **Duplicate records downstream** | At-least-once reprocessing, producer retries without idempotence. Enable idempotence, idempotent sink (upsert/dedup by event id), transactions. |
| 114 | **Messages lost** | `acks=1` + leader failure, unclean election, min ISR=1, consumer committed before processing, retention too short. Fix configs (`acks=all`, RF=3, min ISR=2, unclean=false), commit after processing. |
| 115 | **Out-of-order events** | Multiple partitions without key, retries without idempotence, multi-threaded consumers, retry topics. Key by entity, idempotence, order-aware processing, event-time + comparison column in Pinot. |
| 116 | **Under-replicated partitions** | Broker down/slow disk/network, overloaded follower. Check broker health, ISR flapping, `replica.lag.time.max.ms`, throttle reassignments. |
| 117 | **Disk full on broker** | Retention too long, hot topic, compaction backlog. Reduce retention temporarily, add disks/brokers, rebalance (Cruise Control), enable tiered storage. |
| 118 | ⭐ **Pinot table shows no/old data** | Stream config wrong (topic/broker list), decoder mismatch, consumption paused, schema mismatch (null columns), table in `ERROR` state, time column filter excluding data (ms vs s epoch), server unhealthy. Check Pinot UI → table → segments (CONSUMING), controller/server logs, `consumingSegmentsInfo`. |
| 119 | ⭐ **Pinot query slow** | See Q75: missing index/pruning, too many segments, large group-by, skewed servers, insufficient page cache, GC. Add inverted/range/bloom/star-tree, reduce columns, add replica groups, use `LIMIT`/approximate functions. |
| 120 | **Upsert table returns duplicates / stale rows** | Topic not partitioned by PK, routing not `strictReplicaGroup`, comparison column wrong (not monotonic), partition count changed, multiple replicas mismatch. Fix key, routing, re-ingest. |
| 121 | **Pinot server OOM** | Upsert key map too large, too many consuming segments per server (many partitions), big segments, heap too high leaving no page cache. Add servers, reduce partitions per server, hash PKs, tune thresholds/heap. |
| 122 | **Segments stuck in CONSUMING / commit failing** | Deep store unreachable/permissions, controller unreachable, segment size too large, ZK slow. Check deep store config, controller logs, `forceCommit`. |
| 123 | **Rolling restart caused query failures** | Not waiting for ideal state/external view convergence, replication = 1. Use replication ≥ 2, `minAvailableReplicas`, health checks between nodes. |
| 124 | **A bad deploy produced malformed messages** | DLQ catches non-parsable; Pinot decoder errors skip row (see `realtime.ingestion.errors`), set `continueOnError`, replay from Kafka after fix; enforce schema registry compat. |
| 125 | **Need to GDPR-delete a user** | Kafka: compacted topic tombstones / short retention / crypto-shredding; Pinot: `PurgeTask` on offline segments or soft-delete in upsert; remove from lake. |

---

# Part E — Coding / hands-on prompts

**E1. Write a Python consumer that never loses messages and tolerates duplicates.**
Manual commit after side-effects succeed; idempotent write; DLQ on poison; graceful `close()`. → see [`processor.py`](../project/app/processor.py).

**E2. Show how to get exactly-once Kafka→Kafka.** → [`03_transactions.py`](../examples/kafka/03_transactions.py) (`init_transactions`, `begin`, `send_offsets_to_transaction`, `commit/abort`, `read_committed`).

**E3. Compute consumer lag programmatically.** → [`04_admin_lag.py`](../examples/kafka/04_admin_lag.py).

**E4. Write the Pinot SQL: revenue per minute for the last 30 min; top-5 products per city; funnel conversion.**
```sql
-- per minute
SELECT DATETRUNC('minute', event_time, 'MILLISECONDS') AS m, SUM(amount) AS revenue
FROM order_events WHERE status = 'PAID' AND event_time > ago('PT30M') GROUP BY m ORDER BY m LIMIT 100;

-- funnel conversion in one scan (FILTER clauses)
SELECT COUNT(DISTINCT order_id) FILTER (WHERE status = 'CREATED')   AS created,
       COUNT(DISTINCT order_id) FILTER (WHERE status = 'PAID')      AS paid,
       COUNT(DISTINCT order_id) FILTER (WHERE status = 'DELIVERED') AS delivered
FROM order_events WHERE event_time > ago('PT1H');
-- (use DISTINCTCOUNTHLL(order_id) FILTER ... at larger scale)

-- top-5 products per city (multi-stage engine, window function)
SET useMultistageEngine = true;
SELECT * FROM (
  SELECT city, product_name, revenue,
         ROW_NUMBER() OVER (PARTITION BY city ORDER BY revenue DESC) AS rn
  FROM (
    SELECT city, product_name, SUM(amount) AS revenue
    FROM orders_state WHERE status <> 'CANCELLED' GROUP BY city, product_name
  ) t
) r WHERE rn <= 5;
```

**E5. Write a Pinot realtime table config with upsert from Kafka.** → [`orders_state.table.json`](../project/pinot/orders_state.table.json).

**E6. Add a star-tree index and prove it helps.** → [`order_events.table.json`](../project/pinot/order_events.table.json); compare `numDocsScanned`/`timeUsedMs` after segment commit with `SET useStarTree=false;`.

**E7. Write a partitioner that keeps hot keys balanced.** Key salting: `key = f"{user_id}#{hash(event_id) % N}"` (loses strict ordering per user; aggregate later) — explain trade-off.

**E8. Implement retry topics with back-off.** Consumer on `orders.retry.5s` waits until `now ≥ header.next_attempt` (pause partition meanwhile), on repeated failure forwards to the next tier, finally DLQ with headers (`attempt`, `error`, `orig_topic/partition/offset`).

**E9. Implement an idempotent consumer.** Table/cache of processed `event_id` (unique constraint / Redis `SETNX` with TTL) checked inside the same transaction as the side-effect.

**E10. Create a topic with 6 partitions RF 3 and min ISR 2 using the CLI.**
`kafka-topics.sh --bootstrap-server b:9092 --create --topic t --partitions 6 --replication-factor 3 --config min.insync.replicas=2`

---

# Part F — Behavioural / project questions

1. Walk me through the architecture of the most recent streaming system you built; what were the SLAs?
2. What was the hardest production incident involving Kafka/Pinot? Root cause, detection, fix, prevention?
3. How did you decide partitions, replication and retention? What would you change at 10× scale?
4. How did you test the pipeline (unit, contract, integration with Testcontainers, load, chaos)?
5. How did you handle schema evolution without downtime?
6. How did you make the pipeline observable and what alerts paged you?
7. How did you control cost?
8. Describe a trade-off you made (consistency vs availability, freshness vs cost, exactly-once vs complexity).
9. How did you onboard other teams to produce/consume safely (conventions, schemas, ACLs, self-service topics)?
10. What would you improve in [this project](04-project.md)? *(Avro + registry, Flink for stateful enrichment, transactions, Pinot hybrid + minion compaction, auth on API, Testcontainers CI, Grafana dashboards.)*

---

# Quick revision sheet

**Kafka in 10 lines**
1. Log of records in partitions; order only per partition; key → partition.
2. Replication via leader/followers; ISR; `acks=all` + `min.insync.replicas=2` + RF=3.
3. Consumers pull; groups split partitions; offsets in `__consumer_offsets`.
4. Rebalance = pause; use cooperative-sticky + static membership.
5. At-least-once = process then commit; exactly-once = idempotence + transactions + read_committed.
6. Retention (delete) vs compaction (latest per key, tombstones).
7. KRaft replaces ZooKeeper.
8. Connect (integration), Streams/Flink (processing), Schema Registry (contracts).
9. Monitor lag, URP, offline partitions, request latency.
10. Partition count is a one-way door (increase only; breaks key mapping).

**Pinot in 10 lines**
1. Real-time OLAP, ms latency, high QPS, fresh data.
2. Controller · Broker · Server · Minion · ZK · deep store.
3. Table = segments (columnar, immutable after commit); consuming segments are queryable.
4. Real-time (Kafka) / offline (batch) / hybrid (time boundary).
5. Indexes: sorted, inverted, range, bloom, star-tree, text, JSON, geo.
6. Upsert needs PK + key-partitioned topic + strictReplicaGroup; FULL vs PARTIAL; dedup = first-wins.
7. v1 engine (fast single table) vs v2 multi-stage (joins/windows).
8. Segment size 100–500 MB; flush thresholds; filter on time column for pruning.
9. Scale QPS with replica groups; scale data with servers; Minion for compaction.
10. Debug using response stats (`numEntriesScannedInFilter`, `numDocsScanned`, `timeUsedMs`).

**Numbers to remember**: Kafka single partition ≈ 10 MB/s (ballpark) · message default max ≈ 1 MB · RF=3/minISR=2 · `__consumer_offsets` 50 partitions · Pinot ports 9000/8099/8098/8097 · Pinot default LIMIT 10 · segment 100–500 MB.

---
Prev: [04 — Project](04-project.md) · Back to [README](../README.md)
