# Operations

## The schema

Everything lives in one schema, `pgtasks` unless you name another, as plain SQL files under
`io/github/khopland/pgtasks/postgres/db`. How it gets applied is up to you (`pgtasks.schema-mode`):

- `migrate` (default): the built-in runner applies the files at startup, under an advisory lock,
  so nodes starting together don't race. The application's user owns the schema.
- `validate`: Flyway, Liquibase or a DBA pipeline applies the same files, and the engine only
  checks the version. The runtime user then needs USAGE on the schema, SELECT, INSERT, UPDATE and
  DELETE on its tables, USAGE on `task_id_seq` and EXECUTE on `create_topic`. It needs no DDL
  rights: a topic is a row.
- `none`: nothing is checked.

`pgtasks.task_view` shows each task with its current state, for looking around in SQL.

### Naming the schema

`pgtasks.schema` (or `TaskEngineBuilder.schema(...)`, or the `PostgresTaskStore` constructor) names
the schema. Use it when `pgtasks` clashes with a schema of your own, or when two applications share a
database: each needs its own schema. The name must be a plain lowercase identifier
(`[a-z_][a-z0-9_]*`), at most 50 characters, not starting with `pg_` and not an SQL keyword. The
notification channels follow it (`<schema>_ready`, `<schema>_limit`, `<schema>_topic`), so
applications in one database don't wake each other's workers.

The SQL files write the name as the placeholder `${pgtasks_schema}`. The built-in runner fills it
in. With another tool, pass the same name the engine uses:

- **Flyway**: a Flyway instance of its own, so its version numbers don't clash with the
  application's migrations, with the name as a placeholder. Flyway needs the placeholder even for
  the default name:

  ```java
  Flyway.configure()
      .dataSource(dataSource)
      .locations("classpath:io/github/khopland/pgtasks/postgres/db")
      .schemas("billing_tasks")                                   // the history table goes here
      .placeholders(Map.of("pgtasks_schema", "billing_tasks"))
      .table("flyway_task_history")
      .load()
      .migrate();
  ```

- **Liquibase**: include `io/github/khopland/pgtasks/postgres/db/changelog.xml` from your changelog.
  The changelog parameter `pgtasks_schema` names the schema and defaults to `pgtasks`. Set it before
  the include is parsed: with `spring.liquibase.parameters.pgtasks_schema=billing_tasks`,
  `liquibase.setChangeLogParameter("pgtasks_schema", ...)`, `-Dliquibase.parameter.pgtasks_schema=...`,
  or a `<property name="pgtasks_schema" value="billing_tasks"/>` above the include. The changeSet
  ids carry the name (`billing_tasks-V001`), so two schemas can share one `DATABASECHANGELOG`.
- **A DBA pipeline**: replace the placeholder in each file, for instance with
  `sed 's/\${pgtasks_schema}/billing_tasks/g'`, and run the files in the order of `TaskSchema.FILES`.

### Changing the schema

`V001__tasks_schema.sql` is frozen for 0.1.0. **Never edit a released versioned file**: databases
that applied it would no longer match it. The built-in runner refuses to start when an applied
file's checksum changed, and Flyway and Liquibase do the same. Add the next file instead:

1. `V002__<what>.sql`: the change, written so that 0.1.0 nodes still run against it during a rolling
   deploy, or with `compatible_from` raised if they can't. It ends by recording its version in the
   meta table, like V001. Use `${pgtasks_schema}` for every name.
2. Add it to `TaskSchema.FILES`, as a `changeSet` in `changelog.xml`, and raise `TaskSchema.VERSION`.
3. Functions, views and triggers stay in the `R__` files, which may change at any time: every path
   applies them again when their content changes. Dropping or changing a view's column needs a
   versioned file.
4. `SchemaFreezeTest` holds the checksum of every released versioned file and fails if one changes.
   Add the new file's checksum there when it is released. `SchemaUpgradeTest` applies the files as
   0.1.0 released them (kept under `released/0.1.0` in the test resources), with tasks in every
   state, then the current files, checks the tasks are still usable and runs part of the store TCK
   on the result. A V002 is covered as soon as it is listed; add a `released/<version>` directory
   and a seed when a release changes the schema.

## Actuator: the tasks endpoint and health

Expose it like any endpoint: `management.endpoints.web.exposure.include=health,tasks`.

```
GET    /actuator/tasks                                   topics by module, limits, this node and every node
GET    /actuator/tasks/topics/{topic}                    status, counts, settings and where each came from, schedules
GET    /actuator/tasks/topics/{topic}/dead-letters?after=&limit=
GET    /actuator/tasks/topics/{topic}/failing            what is failing, grouped by error
POST   /actuator/tasks/topics/{topic}/pause | resume | create
POST   /actuator/tasks/topics/{topic}/revive | discard   {"ids": "1,2"}, or {"error": "...", "failedFrom": "...", "failedUntil": "..."}, or {"all": "true"}
POST   /actuator/tasks/topics/{topic}/purge              {"outcome": "succeeded", "before": "2026-01-01T00:00:00Z"}
DELETE /actuator/tasks/topics/{topic}                    a topic no longer used, with its history
GET    /actuator/tasks/tasks/{id}                        a task with its logged attempts
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

## Dashboard

![The dashboard's overview](dashboard.png)

A web page over the tasks endpoint, for Spring MVC applications with Actuator. It's in the starter,
off by default:

```yaml
pgtasks:
  dashboard:
    enabled: true
    path: /pgtasks          # the default
management:
  endpoints.web.exposure.include: health,tasks
```

Then open `http://<host>/pgtasks/`. It shows:

- **Overview**: topics grouped by module, with their counts per state, the age of the oldest ready
  task, failing tasks and dead letters, whether this node's breaker is open, and paused or running.
  Every registered node with its heartbeat (marked late after the lease term) and what it leads,
  and the rate limits.
- **Topic**: counts, what is failing grouped by error, with revive and discard per error, a paged
  list of dead letters to revive or discard by id (or all of them), the topic's effective settings
  and where each came from, its recurring tasks, pause and resume, purge and delete.
- **Task**: look one up by id in the header, with its attempts and errors.
- **Limits**: override a rate, a burst or both, for a while or with no end, and reset. A change
  sends the version the page loaded; if someone else changed the limit meanwhile, the page says so
  and shows the current values instead of overwriting them.

Discards, purges and deletes ask for confirmation, and the ones that can't be undone at scale
(discarding every dead letter, purging, deleting a topic) ask you to type the topic's name.
"Live" refreshes the page every 10 seconds.

**Security.** The page holds no data and has no server logic of its own: it reads and changes
everything through `/actuator/tasks`, so it has exactly the endpoint's security, and shows
payloads, outputs and error messages only as far as `management.endpoint.tasks.show-values`
allows. Secure the dashboard's path like the endpoint, since its pages show the same things. It
renders all data as text, never as HTML, and every response carries a Content-Security-Policy that
allows only its own scripts and styles and no framing. It works offline: no CDN, no fonts.

With Spring Security, the page's changes are POST and DELETE requests to the endpoint, so they
need a CSRF token. The page gets it from `<path>/config.json`, which reads Spring Security's token
for the request and hands it to the page; the page sends it in the header Spring Security names
(`X-CSRF-TOKEN` by default). This needs no configuration, as long as the dashboard's path goes
through the same security filter chain, with CSRF on, as the endpoint. With
`CookieCsrfTokenRepository` the page also falls back to sending the `XSRF-TOKEN` cookie as
`X-XSRF-TOKEN`. If you turned CSRF off for `/actuator/**`, nothing is needed either. For example:

```java
@Bean
SecurityFilterChain security(HttpSecurity http) throws Exception {
    return http
        .authorizeHttpRequests(auth -> auth
            .requestMatchers("/pgtasks/**", "/actuator/tasks/**").hasRole("OPS")
            .anyRequest().authenticated())
        .formLogin(Customizer.withDefaults())
        .build();
}
```

The page finds the endpoint at `<context path><management base path>/tasks`. If a proxy rewrites
paths, set `pgtasks.dashboard.endpoint` to the URL the browser should call. If the endpoint isn't
exposed, the page says so. WebFlux applications aren't supported.

### When Actuator is on a port the browser can't reach

In Kubernetes, Actuator often runs on its own port (`management.server.port`) that only the
kubelet and Prometheus reach, so the page on the application's port can't call it. Then let the
dashboard serve the same operations itself, next to the page:

```yaml
pgtasks:
  dashboard:
    enabled: true
    path: /admin/pgtasks
    api: dashboard          # the API at /admin/pgtasks/api; default: actuator
```

The page is at `/admin/pgtasks/` and its API at `/admin/pgtasks/api/...`, with the same paths,
bodies and answers as `/actuator/tasks` (the tasks endpoint itself needn't be exposed). Values are
still hidden as `management.endpoint.tasks.show-values` says, with `when-authorized` checking the
roles of the request's user, and limit changes still need the current version.

This puts operations that change tasks on the application's port, so the application's own
security must protect the whole path, and it is off unless you ask for it. With Spring Security,
one rule covers the page and its API, and CSRF works as above:

```java
.authorizeHttpRequests(auth -> auth
    .requestMatchers("/admin/pgtasks/**").hasRole("OPS")
    .anyRequest().authenticated())
```

Without Spring Security the dashboard logs a warning at startup. Keep the path off your public
ingress (route it only through an internal one, or reach it with `kubectl port-forward`). Each pod
shows the same topics, counts, dead letters, limits and nodes, since they come from the database;
"running" and breaker state are that pod's own.

## Metrics and alerts

With Micrometer (Spring Boot Actuator sets it up), the engine records:

| Metric | What |
|---|---|
| `pgtasks.push`, `pgtasks.batch`, `pgtasks.task` | Observations (timers, and spans with tracing) of pushes, batches, and each task a handler decides one by one |
| `pgtasks.pushed{topic,status}` | Committed pushes |
| `pgtasks.completed{topic,result}` | Completed tasks: succeeded, filtered, suspended, delayed, throttled, unavailable, retrying, dead-lettered |
| `pgtasks.queue{topic,state}` | Unfinished tasks per state, from the last snapshot |
| `pgtasks.queue.oldest.ready{topic}` | Seconds since the oldest ready task was pushed |
| `pgtasks.queue.failing{topic}`, `pgtasks.queue.oldest.failing{topic}` | Tasks whose last run failed, and since when the longest-failing one has failed |
| `pgtasks.queue.snapshot.age{topic}` | Seconds since the counts were taken |
| `pgtasks.breaker.open{topic}` | 1 while this node's breaker for the topic is open |
| `pgtasks.limit.rate{limit,kind}`, `pgtasks.limit.burst{limit,kind}` | A limit's configured and effective values |
| `pgtasks.limit.wait{topic,limit}` | Time topics waited for their rate limit |

Queue gauges come from snapshots the housekeeping leader takes every minute, so scraping never
counts the queue. Alert on:

- `pgtasks.queue.oldest.ready`: work is waiting longer than it should. It also catches a topic no
  node processes any more.
- `pgtasks.queue.oldest.failing`: something has been failing for a while. Dead letters can take
  days to appear.
- `pgtasks.queue.snapshot.age` growing past a few minutes: no node runs housekeeping.
- `pgtasks.breaker.open`, and the health indicator going DOWN.

## Tracing and correlation

Traces follow a task from the code that pushed it through every topic downstream. Every header the
tracer's propagator writes is stored with the task (`traceparent` and `tracestate`, `baggage`,
`b3`, ...), so W3C, B3 and remote baggage fields all carry over. A trace looks like this:

```
POST /returns
  push assessment            the push, a producer span
    process assessment       the batch
      handle assessment      the task, while its decider runs
        push notify          its downstream work
          process notify
            handle notify
```

A batch continues its tasks' trace when they all share one, such as a batch of one or of tasks
pushed together. A batch of tasks from different traces starts a trace of its own, and each
task's `handle` span continues the trace that task was pushed from, so none of them is cut off
and each task's downstream work stays in its own trace. Tasks get their own span when the handler
goes through `batch.decide`, `decideConcurrently` or `eachTask`, and when they have downstream
work; a handler that only returns `Decisions` for the whole batch is traced as the batch.

The MDC carries `task.topic`, and `task.id` and `task.identifier` while a task's own span is
current (and for batches of one).

For a correlation id without tracing, or next to it, name the MDC keys to carry:

```yaml
pgtasks:
  mdc-headers: korrelasjonsid
```

or `builder.observer(TaskObserver.mdc(List.of("korrelasjonsid")))`. Each key the MDC has when a
task is pushed, typically set by a web filter for the request, is stored as a header of that
name, and is in the MDC again while a handler works on the task, so log lines carry it and
downstream pushes pick it up again. The value comes from the task: a handler that wants to give
downstream work a new one sets the header on the creation. With tracing, a remote baggage field
(`management.tracing.baggage.remote-fields` and `correlation.fields`) does the same through
`baggage`.

Headers are a `jsonb` column, so support can find a flow by its correlation id:

```sql
SELECT * FROM pgtasks.task_view WHERE headers->>'korrelasjonsid' = '...';
```

Add an index on that expression if you look them up often. The endpoint shows header names, and
their values only where `show-values` allows.

## Dead letters

1. Look at what fails: `GET .../topics/{topic}/failing` groups the failing tasks by error.
2. Fix the cause, or let the deploy that fixes it revive them
   (`DeadLetterPolicy.KEEP.withReviveOnNewVersion()`, with the application version from Spring
   Boot's `BuildProperties`, `pgtasks.version`, or `builder.version(...)`).
3. Revive by error type and time range, or by ids. Discard what should never run. Nothing
   revives or discards every dead letter of a topic without `"all": "true"`.

## Retention

Finished tasks are deleted by the housekeeping leader, per topic and outcome, counted from
completion: succeeded after 30 days, filtered, redundant and recreated after 7, failed (discarded
dead letters) never. Change it per topic with `options.retain(Outcome.SUCCEEDED, Period.ofDays(90))`,
for every topic in `TaskGlobals`, or per environment with `pgtasks.topics.<t>.retain.succeeded: 90d`.
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
`pgtasks.notifications=false`; polling still finds the work, within the poll interval. Use
`validate` there too, since the built-in runner's advisory lock is per session.

## Deploys and shutdown

- On shutdown the engine stops claiming, lets running batches finish for
  `pgtasks.shutdown-timeout` (30 s), then hands back what is left without counting an attempt.
- Rolling deploys are safe: nodes of old and new versions share the schema, which records the
  oldest library version it stays compatible with.
- Topic changes (created, paused, resumed, deleted) and rate limit overrides reach every node
  within about a second through LISTEN/NOTIFY, and within one heartbeat (15 s) otherwise.
- One node at a time holds the housekeeping lease (four heartbeats long), which moves to another
  node if it stops renewing it.

## Logging

The library logs through the SLF4J 2 API and brings no backend: Spring Boot's Logback picks it
up without any setup, and elsewhere any SLF4J provider on the classpath does (without one, SLF4J
prints a warning and drops the messages). Loggers are named after the classes, under
`io.github.khopland.pgtasks`. INFO covers lifecycle events such as topics starting and
stopping, schema migrations and lease changes.
