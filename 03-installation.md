# 03 — Installation Guide

Three ways, from easiest to most "production-like":

| Option | Use when | Time |
|---|---|---|
| **A. Docker Compose (recommended)** | Learning + the project in this repo | ~5 min |
| **B. Native tarballs** | You want to understand the processes / no Docker | ~15 min |
| **C. Kubernetes (Helm / Strimzi)** | Production-style deployment | ~30 min |

Ports used: Kafka `29092` (host) / `9092` (docker network) · Kafka UI `8080` · Pinot Controller UI `9000` · Pinot Broker `8099` · API `8000` · ZooKeeper `2181`.

---

## A. Docker Compose (project stack)

### Prerequisites
* **Docker Desktop** (Windows: enable WSL2 backend; macOS; or Docker Engine + compose plugin on Linux) — **≥ 6 GB RAM** assigned to Docker (Pinot ≈ 3 GB, Kafka ≈ 1 GB).
  * Windows: `winget install Docker.DockerDesktop`, reboot, then `docker --version` and `docker compose version`.
* **Python 3.10+** (3.11 tested).

### Steps (PowerShell shown; bash is identical except venv activation)

```powershell
cd kafka-pinot-learning\project

# 1. start everything: Kafka, topics, Kafka UI, ZooKeeper, Pinot (controller/broker/server), then auto-create Pinot tables
docker compose up -d
docker compose ps                    # wait until pinot-* are "healthy" (1-2 minutes)
docker compose logs -f pinot-init    # should end without errors (tables created)

# 2. Python environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1         # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

### Verify

```powershell
# Kafka topics exist
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
# Pinot is healthy and tables exist
curl http://localhost:9000/health
curl http://localhost:9000/tables
```
Open: Kafka UI <http://localhost:8080>, Pinot UI <http://localhost:9000> (Cluster Manager → Tables → `order_events_REALTIME`, `orders_state_REALTIME`; Query Console).

### Stop / reset

```powershell
docker compose down        # stop, keep nothing (no volumes are defined → clean slate next time)
docker compose down -v --remove-orphans
```

### Troubleshooting

| Symptom | Fix |
|---|---|
| `pinot-server` unhealthy / OOM-killed (exit 137) | Increase Docker memory to ≥ 6 GB (Settings → Resources) or lower `-Xmx` in compose. |
| `pinot-init` failed | Re-run only that step: `docker compose up pinot-init` (idempotent, uses `-update`). |
| Python can't connect to Kafka (`Connection refused`, `localhost:29092`) | Check `docker compose ps`, port 29092 not in use; inside containers always use `kafka:9092`. |
| Producer connects but metadata errors mention `kafka:9092` | You're using the *internal* listener from the host – use `localhost:29092`. |
| Pinot table has 0 rows | Producer + processor must both be running; check `orders.enriched` in Kafka UI; check table's *Consuming segments* in Pinot UI. |
| Port already in use (8080, 9000) | Edit left side of `ports:` in `docker-compose.yml`. |
| `Cannot connect to the Docker daemon` | Start Docker Desktop. |

---

## B. Native installation (no Docker)

Requires **Java 17** (Kafka 3.8 supports 11/17/21; Pinot 1.x supports 11/17/21). Check `java -version`.

### Kafka (KRaft)

```bash
curl -O https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz && cd kafka_2.13-3.8.0

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties        # listens on localhost:9092

# new terminal
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders.raw --partitions 6
```
Windows: use WSL2 (recommended), because the `.bat` scripts under `bin\windows` are less maintained.

### Pinot

```bash
curl -O https://archive.apache.org/dist/pinot/apache-pinot-1.3.0/apache-pinot-1.3.0-bin.tar.gz
tar -xzf apache-pinot-1.3.0-bin.tar.gz && cd apache-pinot-1.3.0-bin

# three terminals (or background them)
bin/pinot-admin.sh StartZookeeper -zkPort 2181
bin/pinot-admin.sh StartController -zkAddress localhost:2181
bin/pinot-admin.sh StartBroker     -zkAddress localhost:2181
bin/pinot-admin.sh StartServer     -zkAddress localhost:2181

# create schema+table (edit broker.list to localhost:9092 in the table config first)
bin/pinot-admin.sh AddTable -schemaFile order_events.schema.json -tableConfigFile order_events.table.json -exec
```

One-liner demo with built-in sample data and a Kafka stream: `bin/quick-start-streaming.sh` (starts everything in one JVM; UI at :9000).

---

## C. Kubernetes (production-style)

```bash
# Kafka via Strimzi operator
kubectl create namespace kafka
kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
# then apply a Kafka + KafkaNodePool custom resource (3 brokers, replication 3, min.insync.replicas 2)

# Pinot via official Helm chart
helm repo add pinot https://raw.githubusercontent.com/apache/pinot/master/helm
kubectl create namespace pinot
helm install pinot pinot/pinot -n pinot \
  --set controller.replicaCount=2 --set broker.replicaCount=2 --set server.replicaCount=3 \
  --set zookeeper.replicaCount=3
kubectl port-forward -n pinot svc/pinot-controller 9000:9000
```
Production checklist: persistent volumes for servers & ZK, **deep store on S3/GCS/ADLS** (not local disk), replication ≥ 2/3, rack awareness, TLS+auth, Prometheus JMX exporters, resource requests/limits, PodDisruptionBudgets, anti-affinity.

---

## Python client libraries used

| Package | Purpose |
|---|---|
| `confluent-kafka` | Producer/consumer/admin (librdkafka-based, fastest Python client). Alternative: `kafka-python`, `aiokafka`. |
| `pinotdb` | DB-API / SQLAlchemy driver for Pinot (used in the example). |
| `requests` | HTTP query to broker. |
| `fastapi`, `uvicorn` | Serving the analytics API. |
| `pytest`, `httpx` | Tests. |

---
Prev: [02 — Pinot](02-pinot.md) · Next: [04 — Project](04-project.md)
