#!/usr/bin/env bash
# Calls the tasks endpoint as an operator would through the dashboard: a session, and the CSRF
# token the dashboard reads from <path>/config.json.
#   scripts/ops.sh [port] GET  topics/orders.ship
#   scripts/ops.sh [port] POST topics/orders.ship/revive '{"all":"true"}'
set -euo pipefail
cd "$(dirname "$0")/.."
port=8081
if [[ $1 =~ ^[0-9]+$ ]]; then port=$1; shift; fi
method=$1 path=$2 body=${3:-}
[[ $method == POST && -z $body ]] && body={}
jar=run/ops-$port.jar
config=$(curl -sf -c "$jar" -b "$jar" -u ops:ops "localhost:$port/pgtasks/config.json")
token=$(sed -E 's/.*"token":"([^"]+)".*/\1/' <<< "$config")
header=$(sed -E 's/.*"header":"([^"]+)".*/\1/' <<< "$config")
args=(-s -w '\n%{http_code}\n' -c "$jar" -b "$jar" -u ops:ops -X "$method" -H "$header: $token")
[[ -n $body ]] && args+=(-H 'Content-Type: application/json' -d "$body")
curl "${args[@]}" "localhost:$port/actuator/tasks/$path"
