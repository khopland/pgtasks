#!/usr/bin/env bash
# Every 30 s: unfinished tasks per topic and state, order statuses, and each instance's health.
cd "$(dirname "$0")/.."
end=$(( $(date +%s) + ${1:-2400} ))
while (( $(date +%s) < end )); do
  {
    echo "== $(date +%T)"
    docker exec orders-postgres psql -U postgres -d orders -AtF' ' -c \
      "SELECT 'queue', topic, state, count(*) FROM pgtasks.task_view WHERE completed_at IS NULL GROUP BY 2,3 ORDER BY 2,3;
       SELECT 'orders', status, count(*) FROM orders.orders GROUP BY 2 ORDER BY 2;" 2>&1 | tr '\n' ';'
    echo
    for p in 8081 8082 8083; do
      printf '%s %s; ' "$p" "$(curl -s -m 2 localhost:$p/actuator/health | grep -o '"tasks":{"details":{[^}]*}' | grep -o '"leader":[a-z]*\|"notifications":"[a-z]*"\|"stuckClaims":[0-9]*' | tr '\n' ' ')"
    done
    echo
  } >> run/samples.log
  sleep 30
done
