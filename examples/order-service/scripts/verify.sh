#!/usr/bin/env bash
# Checks that every order ended in the right state and counts side effects.
# Run it once the load has stopped and the queues have drained.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p run
curl -sf localhost:9091/dump > run/stub-calls.csv
psql() { docker exec -i orders-postgres psql -U postgres -d orders -v ON_ERROR_STOP=1 -X -q "$@"; }
{
  echo "CREATE TEMP TABLE stub_call (endpoint text, order_id text, idempotency_key text, calls int, ok int);"
  echo "COPY stub_call FROM STDIN WITH (FORMAT csv, HEADER true);"
  cat run/stub-calls.csv
  echo '\.'
  cat <<'SQL'
\pset footer off
\echo '== orders by status'
SELECT status, count(*) FROM orders.orders GROUP BY status ORDER BY status;

\echo '== orders not in a final state (COMPLETED, CANCELLED)'
SELECT count(*) AS in_flight, min(created_at) AS oldest FROM orders.orders WHERE status NOT IN ('COMPLETED', 'CANCELLED');

\echo '== completed orders missing a step'
SELECT count(*) FILTER (WHERE paid_at IS NULL) AS unpaid, count(*) FILTER (WHERE reserved_at IS NULL) AS unreserved,
       count(*) FILTER (WHERE shipped_at IS NULL) AS unshipped, count(*) FILTER (WHERE notified_at IS NULL) AS unnotified
FROM orders.orders WHERE status = 'COMPLETED';

\echo '== cancelled orders: stock released, customer told, never charged or shipped'
SELECT count(*) AS cancelled,
       count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM orders.stock_movement m WHERE m.order_id = o.id AND m.kind = 'release')) AS not_released,
       count(*) FILTER (WHERE notified_at IS NULL) AS not_told,
       count(*) FILTER (WHERE paid_at IS NOT NULL OR shipped_at IS NOT NULL) AS charged_or_shipped
FROM orders.orders o WHERE status = 'CANCELLED';

\echo '== stock: reserved per sku must equal the open reservations'
SELECT count(*) AS skus_off FROM orders.stock s
WHERE s.reserved <> coalesce((SELECT sum(l.quantity) FROM orders.order_line l
                              JOIN orders.stock_movement r ON r.order_id = l.order_id AND r.kind = 'reserve'
                              WHERE l.sku = s.sku
                                AND NOT EXISTS (SELECT 1 FROM orders.stock_movement x WHERE x.order_id = l.order_id AND x.kind = 'release')), 0);

\echo '== notifications our side recorded more than once'
SELECT kind, count(*) AS orders, count(*) FILTER (WHERE sent_count > 1) AS duplicated FROM orders.notification_log GROUP BY kind ORDER BY kind;

\echo '== partner side effects: distinct idempotency keys with a success per order (1 is right)'
SELECT endpoint, count(DISTINCT order_id) AS orders, sum(calls) AS calls, count(*) AS keys,
       count(*) FILTER (WHERE ok > 1) AS keys_succeeded_twice
FROM stub_call GROUP BY endpoint ORDER BY endpoint;
SELECT endpoint, effects, count(*) AS orders FROM (
    SELECT endpoint, order_id, count(*) FILTER (WHERE ok > 0) AS effects FROM stub_call GROUP BY endpoint, order_id
) e GROUP BY endpoint, effects ORDER BY endpoint, effects;

\echo '== shipments for orders that are not paid, payments for declined orders'
SELECT count(*) AS shipped_unpaid FROM (SELECT DISTINCT order_id FROM stub_call WHERE endpoint = 'shipments' AND ok > 0) s
JOIN orders.orders o ON o.id = s.order_id WHERE o.paid_at IS NULL;

\echo '== stub effects vs our records'
SELECT count(*) FILTER (WHERE o.shipped_at IS NULL) AS shipped_at_carrier_not_recorded
FROM (SELECT DISTINCT order_id FROM stub_call WHERE endpoint = 'shipments' AND ok > 0) s JOIN orders.orders o ON o.id = s.order_id;

\echo '== pgtasks: unfinished tasks per topic and state'
SELECT topic, state, count(*) FROM pgtasks.task_view WHERE completed_at IS NULL GROUP BY topic, state ORDER BY topic, state;

\echo '== pgtasks: finished tasks per topic and outcome'
SELECT topic, state, count(*) FROM pgtasks.task_view WHERE completed_at IS NOT NULL GROUP BY topic, state ORDER BY topic, state;

\echo '== reconcile: runs per minute (about 1 is right)'
SELECT runs, count(*) AS minutes FROM (SELECT date_trunc('minute', ran_at), count(*) AS runs FROM orders.reconcile_run GROUP BY 1) r
GROUP BY runs ORDER BY runs;
SQL
} | psql
