# Apache Kafka + Apache Pinot — Learning Path & Industry Project

A self-contained course: theory → concepts → runnable examples → an industry-style real-time analytics project → interview preparation.

| # | Chapter | What you get |
|---|---|---|
| 1 | [Apache Kafka](docs/01-kafka.md) | Architecture, partitions, replication, producers/consumers, delivery semantics, exactly-once, compaction, ecosystem, security, ops, CLI cheat-sheet |
| 2 | [Apache Pinot](docs/02-pinot.md) | Architecture, segments, real-time ingestion, indexes (star-tree…), upsert/dedup, query engines, tuning, REST cheat-sheet, comparisons |
| 3 | [Installation](docs/03-installation.md) | Docker Compose, native tarballs, Kubernetes; troubleshooting |
| 4 | [Industry project](docs/04-project.md) | Real-time order analytics: Kafka → processor → Pinot (append + upsert) → REST API; experiments & production checklist |
| 5 | [Interview Q&A](docs/05-interview-questions.md) | 125+ questions with answers (fundamentals → system design → troubleshooting), coding prompts, revision sheet |
| 6 | [Pinot storage model in depth](docs/06-pinot-storage-model.md) | Tables, segments, tenants, cluster, controller/ZK/Helix/broker/server/minion — explained with one worked sample dataset |

## Suggested 2-week study plan

| Days | Do |
|---|---|
| 1–2 | Read chapter 1; run `examples/kafka/01…02` (producer, consumer groups; start 2 consumers and watch rebalances) |
| 3 | Kafka delivery semantics: `03_transactions.py`, `04_admin_lag.py`; break things (kill consumer, reset offsets) |
| 4–5 | Read chapter 2; run Pinot queries in [`examples/pinot/queries.sql`](examples/pinot/queries.sql) |
| 6–9 | Build/run the project (chapter 4), do every experiment |
| 10–11 | Extend: Avro + Schema Registry, Grafana, hybrid table |
| 12–14 | Interview Q&A aloud, system-design drills (Part C/D) |

## Quick start

```powershell
cd project
docker compose up -d                                   # Kafka + Pinot + tables (needs Docker, ~6 GB RAM)
python -m venv .venv ; .\.venv\Scripts\Activate.ps1 ; pip install -r requirements.txt
pytest -q                                              # unit tests (no infra needed)
python -m app.producer --rate 50                       # terminal 1
python -m app.processor                                # terminal 2
uvicorn app.api:app --port 8000                        # terminal 3 → http://localhost:8000/docs
```
UIs: Kafka UI <http://localhost:8080> · Pinot <http://localhost:9000>

## Layout

```text
docs/        chapters 1-5
examples/
  kafka/     01_producer · 02_consumer_group · 03_transactions · 04_admin_lag
  pinot/     queries.sql · query_client.py
project/     docker-compose.yml · pinot/ (schemas, table configs) · app/ (producer, processor, queries, api) · tests/
```

> Note: the local stack uses plaintext listeners and no auth — for learning only. See the production checklist in chapter 4.
