"""Pinot example - query the broker over HTTP and via the pinotdb DB-API driver.

pip install requests pinotdb
Run: python examples/pinot/query_client.py
"""
import requests

BROKER = "http://localhost:8099"

# --- 1) raw HTTP: POST /query/sql -------------------------------------------
resp = requests.post(f"{BROKER}/query/sql", json={
    "sql": "SELECT status, COUNT(*) AS n FROM order_events GROUP BY status ORDER BY n DESC",
    "queryOptions": "timeoutMs=5000",
}, timeout=10)
resp.raise_for_status()
body = resp.json()
print("columns:", body["resultTable"]["dataSchema"]["columnNames"])
for row in body["resultTable"]["rows"]:
    print(row)
# Useful stats that explain performance:
print({k: body.get(k) for k in ("numDocsScanned", "numEntriesScannedInFilter", "totalDocs", "timeUsedMs",
                                 "numSegmentsQueried", "numSegmentsMatched")})

# --- 2) DB-API driver --------------------------------------------------------
from pinotdb import connect  # noqa: E402

conn = connect(host="localhost", port=8099, path="/query/sql", scheme="http")
cur = conn.cursor()
cur.execute("SELECT category, SUM(amount) FROM orders_state GROUP BY category")
for row in cur.fetchall():
    print(row)
