#!/usr/bin/env bash
# Chaos during a load run (start it together with the load generator). Times are minutes from start.
#   3  kill -9 instance 2 mid-batch; restart it a minute later
#   7  restart Postgres
#  10  partners fail every call (500) for 3 minutes
#  15  partners down (connection refused) for 2 minutes
#  19  pause orders.ship for 2 minutes
#  23  rolling restart, one instance at a time, with more payment concurrency
#  29  stop rejecting addresses (the "fix" for the shipments that were dead-lettered)
set -uo pipefail
cd "$(dirname "$0")/.."
start=$(date +%s)
log() { echo "$(date +%T) [+$(( ($(date +%s) - start) / 60 ))m] $*" | tee -a run/chaos.log; }
at() { local wait=$(( start + $1 * 60 - $(date +%s) )); (( wait > 0 )) && sleep "$wait"; }
stub() { curl -s -XPOST "localhost:9091/config?$1" > /dev/null; log "stub: $1"; }

log "chaos starts"
at 3;  log "kill -9 instance 2";    scripts/cluster.sh kill 2 | tee -a run/chaos.log
at 4;  log "restart instance 2";    scripts/cluster.sh start 2 | tee -a run/chaos.log
at 7;  log "restart postgres";      docker restart orders-postgres > /dev/null; log "postgres back"
at 10; stub "fail=1.0"
at 13; stub "fail=0.02"
at 15; stub "down=true"
at 17; stub "down=false"
at 19; log "pause orders.ship";     scripts/ops.sh 8081 POST topics/orders.ship/pause | tee -a run/chaos.log
at 21; log "resume orders.ship";    scripts/ops.sh 8082 POST topics/orders.ship/resume | tee -a run/chaos.log
at 23; for n in 1 2 3; do
         log "rolling restart: instance $n"
         scripts/cluster.sh stop "$n" | tee -a run/chaos.log
         scripts/cluster.sh start "$n" --pgtasks.topics.orders.payment.concurrency=4 --pgtasks.topics.orders.payment.batch-size=16 | tee -a run/chaos.log
         sleep 30
       done
at 29; stub "reject=0"
log "chaos done"
