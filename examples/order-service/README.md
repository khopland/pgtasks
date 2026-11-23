# Order service: pgtasks in a realistic application

An order service built on pgtasks the way an outside team would use it: the Spring Boot starter
from the BOM, Flyway, Spring Security, Actuator, Micrometer with tracing, and the dashboard. It
exists to find what the library's own tests can't; what it found is in [FINDINGS.md](FINDINGS.md).

It is a standalone Maven project, not a module of the root build.

## The flow

```
POST /orders ──► orders.payment ──┐ marker "<id>:paid"      ┐
   (one tx)  ├─► orders.stock ────┤ marker "<id>:reserved"  ├─ junction (Counts.ANY) releases
             └─► orders.ship  (suspended) ◄─────────────────┘  the order's own ship task
                    └─► customer.notify "shipped", and "feedback" 30 s later (delayed)
declined payment ──► orders.ship REPLACE (cancel: release stock, waits with later() for the stock step)
                        └─► customer.notify "declined"
orders.reconcile: about every minute, once per cluster (each run pushes the next), counts orders in flight > 5 min
```

| Topic | Handler | Settings |
|---|---|---|
| `orders.payment` | gateway call per task, `decideConcurrently(16)`; 5xx/timeout → `failed`, not counted (`Retry.notCounting`), 429 → `later(Retry-After)`, 402 → cancel | dedup key `pay:<id>`, backoff 2 s..1 min, gives up after 1 h (YAML) |
| `orders.stock` | stock reservation in our own tables, staged in a `BatchResource` (`StagedWrites`) | |
| `orders.ship` | carrier call for the order; markers are filtered; 422 → `failed` (3 attempts, then dead letter) | |
| `customer.notify` | mailer call | `SINGULAR_BY_IDENTIFIER` per customer, dedup keys |
| `orders.reconcile` | counts stuck orders, then pushes the next run a minute later (SUPERSEDE) | |

Every write to our own tables from a handler goes through `StagedWrites`, a `BatchResource` that
runs the SQL on the completion's connection, so it commits exactly when the task's decision does.
Calls to partners carry `task.idempotencyKey()` as `Idempotency-Key`.

### Schema and roles

Two Postgres roles, as a team with a DBA has them: `orders_owner` runs all migrations, the
application runs as `orders_app` without DDL rights. Both schemas go through Flyway
(`PgtasksConfig.migrations`): the pgtasks files with a Flyway instance of their own
(`flyway_pgtasks_history` in schema `pgtasks`), then the application's migrations, which grant
`orders_app` USAGE on `pgtasks` and EXECUTE on `create_topic`. Table and sequence grants come from
`ALTER DEFAULT PRIVILEGES` in `docker/init.sql`. The engine runs with `pgtasks.schema-mode: validate`.

## Running it

Requirements: Java 25, Maven, Docker (colima works).

```bash
# 1. pgtasks itself, into a private local repository, so other checkouts can't overwrite the snapshot
(cd ../.. && mvn install -DskipTests -Dmaven.repo.local=$HOME/.m2-pgtasks-app-trial -Dmaven.repo.local.tail=$HOME/.m2/repository)

# 2. the app
mvn package -Dmaven.repo.local=$HOME/.m2-pgtasks-app-trial -Dmaven.repo.local.tail=$HOME/.m2/repository

# 3. Postgres, the partner stub, three instances (8081-8083)
docker compose up -d --wait
java stub/Stub.java > run/stub.log 2>&1 &
for n in 1 2 3; do scripts/cluster.sh start $n; done
```

- API: `curl -u api:api -H 'Content-Type: application/json' -d '{"requestId":"r1","customerId":"c-1","lines":[{"sku":"sku-1","quantity":2}]}' localhost:8081/orders`
- Dashboard: <http://localhost:8081/pgtasks/>, user `ops`/`ops`.
- Prometheus: `localhost:8081/actuator/prometheus`; health: `localhost:8081/actuator/health`.
- Operator calls from the shell, with the dashboard's session and CSRF token: `scripts/ops.sh 8081 POST topics/orders.ship/pause`.
- The stub: `curl -XPOST 'localhost:9091/config?fail=1.0'` (every call 500), `?down=true` (connection refused), `?reject=0`; `curl localhost:9091/stats`.

### Load, chaos, verification

```bash
java scripts/LoadGen.java --rate 40 --minutes 32 --burst-every 120 --burst 600 > run/load.log &
scripts/chaos.sh &            # kill -9, Postgres restart, partner outage, pause, rolling restart; see the script
# when the load is done and the queues have drained:
scripts/verify.sh
```

The load generator places 40 orders a second, with a burst of 300 a second for 2 s every 2
minutes, spread round robin over the instances (skipping one that refuses connections), and
resends 2 % of requests with an earlier request id. `verify.sh` checks every order's final state,
stock bookkeeping, notifications, the stub's side effects per order and idempotency key, what is
left in the queues, and that the recurring task ran once a minute.

### Tests

`mvn test` runs `OrderFlowTest` (the real configurers on the in-memory store with
`TaskEngineExtension`, fake partners, and staged SQL recorded instead of run) and
`OrderServicePostgresTest` (Testcontainers: an order and its tasks commit and roll back together,
the runtime role without DDL rights). With colima, Testcontainers needs
`DOCKER_HOST=unix://$HOME/.config/colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`.
