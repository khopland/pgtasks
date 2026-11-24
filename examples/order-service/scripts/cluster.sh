#!/usr/bin/env bash
# Starts and stops app instances as plain JVMs: instance N listens on 808N.
#   scripts/cluster.sh start 1 [--spring.args...]   start instance 1 (extra args go to Spring)
#   scripts/cluster.sh stop 1                       SIGTERM: graceful shutdown
#   scripts/cluster.sh kill 1                       SIGKILL: the node dies mid-batch
#   scripts/cluster.sh status
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p run
JAR=${JAR:-target/order-service-1.0.0.jar}
command=${1:?start|stop|kill|status}
n=${2:-}
pidfile() { echo "run/app-$1.pid"; }

case "$command" in
  start)
    shift 2
    port=$((8080 + n))
    if [[ -f $(pidfile "$n") ]] && kill -0 "$(cat "$(pidfile "$n")")" 2>/dev/null; then
      echo "instance $n already runs"; exit 1
    fi
    nohup java -Xmx512m -jar "$JAR" --server.port="$port" "$@" >> "run/app-$n.log" 2>&1 &
    echo $! > "$(pidfile "$n")"
    for _ in $(seq 1 60); do
      curl -sf "localhost:$port/actuator/health/liveness" > /dev/null 2>&1 && { echo "instance $n up on $port (pid $(cat "$(pidfile "$n")"))"; exit 0; }
      sleep 0.5
    done
    echo "instance $n did not come up; see run/app-$n.log"; exit 1 ;;
  stop|kill)
    pid=$(cat "$(pidfile "$n")")
    if [[ $command == kill ]]; then kill -9 "$pid"; else kill "$pid"; fi
    while kill -0 "$pid" 2>/dev/null; do sleep 0.2; done
    rm -f "$(pidfile "$n")"
    if [[ $command == kill ]]; then echo "instance $n killed"; else echo "instance $n stopped"; fi ;;
  status)
    for f in run/app-*.pid; do
      [[ -e $f ]] || continue
      i=${f#run/app-}; i=${i%.pid}
      if kill -0 "$(cat "$f")" 2>/dev/null; then
        echo "instance $i: pid $(cat "$f"), $(curl -s "localhost:$((8080 + i))/actuator/health" | grep -o '"status":"[A-Z_]*"' | tail -1)"
      else
        echo "instance $i: dead"
      fi
    done ;;
esac
