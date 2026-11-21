# Operations

## The schema

Everything lives in the `tasks` schema, as plain SQL files under
`io/github/khopland/task2/postgres/db`. How it gets applied is up to you (`task.schema-mode`):

- `migrate` (default): the built-in runner applies the files at startup, under an advisory lock,
  so nodes starting together don't race. The application's user owns the schema.
- `validate`: Flyway, Liquibase or a DBA pipeline applies the same files, and the engine only
  checks the version. For Liquibase, include `io/github/khopland/task2/postgres/db/changelog.xml`. For
  Flyway, use a Flyway instance of its own with that location, schema `tasks` and a history
  table such as `flyway_task_history`. The runtime user then needs USAGE on the schema, SELECT,
  INSERT, UPDATE and DELETE on its tables, USAGE on `task_id_seq` and EXECUTE on
  `create_topic`. It needs no DDL rights: a topic is a row.
- `none`: nothing is checked.

`tasks.task_view` shows each task with its current state, for looking around in SQL.

## Actuator: the tasks endpoint and health

Expose it like any endpoint: `management.endpoints.web.exposure.include=health,tasks`.

```
GET    /actuator/tasks                                   topics by module, limits, this node
GET    /actuator/tasks/topics/{topic}                    status, counts, settings and where each came from, schedules
GET    /actuator/tasks/topics/{topic}/dead-letters?after=&limit=
GET    /actuator/tasks/topics/{topic}/failing            what is failing, grouped by error
POST   /actuator/tasks/topics/{topic}/pause | resume | create
POST   /actuator/tasks/topics/{topic}/revive | discard   {"ids": "1,2"}, or {"error": "...", "failedFrom": "...", "failedUntil": "..."}, or {"all": "true"}
POST   /actuator/tasks/topics/{topic}/purge              {"outcome": "succeeded", "before": "2026-01-01T00:00:00Z"}
DELETE /actuator/tasks/topics/{topic}                    a topic no longer used, with its history
GET    /actuator/tasks/tasks/{id}
GET    /actuator/tasks/limits/{limit}
POST   /actuator/tasks/limits/{limit}                    {"rate": "20/s", "burst": 10, "until": "PT2H", "version": 3}
DELETE /actuator/tasks/limits/{limit}?version=3          back to the configured values
```

Payloads, outputs and error messages often contain personal data, so the endpoint hides them
unless `management.endpoint.tasks.show-values` allows it: `never` (default), `when-authorized`
(with `management.endpoint.tasks.roles`), or `always`. Errors keep their exception type, so you
can still tell failures apart and revive them by type. Limit changes carry the limit's
`version`, so two operators can't overwrite each other: a stale one gets 409. Everything here is
also library API, on `engine.admin()` and `engine.limits()`.

The `tasks` health indicator is UP while the engine runs and its heartbeat is on time. It is
DOWN if the heartbeat is late or the database can't be reached, and OUT_OF_SERVICE before start
and after stop. Its details show leadership, notifications, claims stuck past their deadline and
open breakers, without changing the status.

## Metrics and alerts

With Micrometer (Spring Boot Actuator sets it up), the engine records:

| Metric | What |
|---|---|
| `task.push`, `task.batch` | Observations (timers, and spans with tracing) of pushes and batches |
| `task.pushed{topic,status}` | Committed pushes |
| `task.completed{topic,result}` | Completed tasks: succeeded, filtered, suspended, delayed, throttled, unavailable, retrying, dead-lettered |
| `task.queue{topic,state}` | Unfinished tasks per state, from the last snapshot |
| `task.queue.oldest.ready{topic}` | Seconds since the oldest ready task was pushed |
| `task.queue.failing{topic}`, `task.queue.oldest.failing{topic}` | Tasks whose last run failed, and since when the longest-failing one has failed |
| `task.queue.snapshot.age{topic}` | Seconds since the counts were taken |
| `task.breaker.open{topic}` | 1 while this node's breaker for the topic is open |
| `task.limit.rate{limit,kind}`, `task.limit.burst{limit,kind}` | A limit's configured and effective values |
| `task.limit.wait{topic,limit}` | Time topics waited for their rate limit |

Queue gauges come from snapshots the housekeeping leader takes every minute, so scraping never
counts the queue. Alert on:

- `task.queue.oldest.ready`: work is waiting longer than it should. It also catches a topic no
  node processes any more.
- `task.queue.oldest.failing`: something has been failing for a while. Dead letters can take
  days to appear.
- `task.queue.snapshot.age` growing past a few minutes: no node runs housekeeping.
- `task.breaker.open`, and the health indicator going DOWN.

Traces follow a task from the code that pushed it through every topic downstream: the pushing
code's W3C `traceparent` is stored with the task. MDC carries `task.topic`, and `task.id` and
`task.identifier` for batches of one.

## Dead letters

1. Look at what fails: `GET .../topics/{topic}/failing` groups the failing tasks by error.
2. Fix the cause, or let the deploy that fixes it revive them
   (`DeadLetterPolicy.KEEP.withReviveOnNewVersion()`, with the application version from Spring
   Boot's `BuildProperties`, `task.version`, or `builder.version(...)`).
3. Revive by error type and time range, or by ids. Discard what should never run. Nothing
   revives or discards every dead letter of a topic without `"all": "true"`.

## Retention

Finished tasks are deleted by the housekeeping leader, per topic and outcome, counted from
completion: succeeded after 30 days, filtered, redundant and recreated after 7, failed (discarded
dead letters) never. Change it per topic with `options.retain(Outcome.SUCCEEDED, Period.ofDays(90))`,
for every topic in `TaskGlobals`, or per environment with `task.topics.<t>.retain.succeeded: 90d`.
Each run deletes for at most 30 s and the next one goes on from where it stopped, so a large
backlog after downtime can't hold up housekeeping.

## Connections

Each node uses the application's `DataSource`:

- Claims and completions use a connection each, at most `maxInFlightBatches` at once: by default
  half the pool, up to 8 (5 with Spring Boot's default pool of 10). Handlers hold no connection
  of the engine's while they run.
- Housekeeping uses up to 4: the heartbeat, the regular jobs, retention and reloads.
- Notifications listen on a long-lived connection. With HikariCP it is opened outside the pool;
  with other pools it takes one of the pool's.

A heartbeat that can't get a connection for long enough (10 minutes by default) lets other nodes
requeue this node's tasks while it still runs them. The engine warns at startup if
`maxInFlightBatches` leaves too little room in the pool.

LISTEN needs a session. Behind PgBouncer in transaction mode, give notifications a direct
connection (`builder.notifier(new PostgresNotifier(...))`) or turn them off with
`task.notifications=false`; polling still finds the work, within the poll interval. Use
`validate` there too, since the built-in runner's advisory lock is per session.

## Deploys and shutdown

- On shutdown the engine stops claiming, lets running batches finish for
  `task.shutdown-timeout` (30 s), then hands back what is left without counting an attempt.
- Rolling deploys are safe: nodes of old and new versions share the schema, which records the
  oldest library version it stays compatible with.
- Topic changes (created, paused, resumed, deleted) and rate limit overrides reach every node
  within about a second through LISTEN/NOTIFY, and within one heartbeat (15 s) otherwise.
- One node at a time holds the housekeeping lease (four heartbeats long), which moves to another
  node if it stops renewing it.

## Logging

The library logs through `System.Logger`. In Spring Boot that reaches Logback through
jul-to-slf4j without any setup. Elsewhere, add `org.slf4j:slf4j-jdk-platform-logging` to send
it to SLF4J, or configure `java.util.logging`. Loggers are named after the classes, under
`io.github.khopland.task2`.
