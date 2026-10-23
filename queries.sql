-- Apache Pinot SQL cheat-sheet. Paste into Query Console: http://localhost:9000/#/query
-- (Run the project stack + producer first so the tables have data.)

-- 1. Basics ------------------------------------------------------------------
SELECT * FROM order_events LIMIT 10;
SELECT COUNT(*) FROM order_events;

-- 2. Filters + aggregation (inverted index on status/category) -----------------
SELECT category, COUNT(*) AS events, SUM(amount) AS revenue
FROM order_events
WHERE status = 'PAID'
GROUP BY category
ORDER BY revenue DESC;

-- 3. Time bucketing: revenue per minute, last 30 minutes ----------------------
SELECT DATETRUNC('minute', event_time, 'MILLISECONDS') AS minute_ts,
       COUNT(*) AS orders, SUM(amount) AS revenue
FROM order_events
WHERE status = 'PAID' AND event_time > ago('PT30M')
GROUP BY minute_ts
ORDER BY minute_ts;

-- 4. Latest state per order (upsert table: exactly one row per order_id) ------
SELECT status, COUNT(*) AS orders
FROM orders_state
GROUP BY status;

SELECT * FROM orders_state WHERE order_id = 'O-REPLACE_ME';

-- 5. Approximate / sketch aggregations (fast, bounded memory) -----------------
SELECT DISTINCTCOUNTHLL(user_id) AS approx_unique_users,
       PERCENTILETDIGEST(amount, 95) AS p95_order_value
FROM order_events;

-- 6. Show how a query was executed: look at numDocsScanned / numEntriesScannedInFilter
--    in the "Response Stats" tab. Star-tree-eligible queries scan very few docs.
SET useMultistageEngine = false;
SELECT city, SUM(amount) FROM order_events WHERE status = 'PAID' GROUP BY city;

-- 7. Multi-stage engine (v2): joins, window functions, sub-queries -------------
SET useMultistageEngine = true;
SELECT s.city, s.status, COUNT(*) AS orders
FROM orders_state s
JOIN (SELECT DISTINCT user_id FROM order_events WHERE status = 'DELIVERED') d
  ON s.user_id = d.user_id
GROUP BY s.city, s.status
ORDER BY orders DESC
LIMIT 20;

SET useMultistageEngine = true;
SELECT order_id, status, event_time,
       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_time) AS step
FROM order_events
LIMIT 20;

-- 7b. EXPLAIN
EXPLAIN PLAN FOR SELECT city, SUM(amount) FROM order_events WHERE status = 'PAID' GROUP BY city;
