# Changelog

What changes in each release of pgtasks. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, a minor version may
break the API; the schema only ever moves forward, with migrations the store applies itself.

## [Unreleased]

### Changed

- Identifiers, references and dedup keys take at most 2,048 bytes in UTF-8
  (`Creation.MAX_KEY_BYTES`). A longer one is refused when it's pushed, with a message that
  says why, instead of failing in Postgres's index (#38).
- Retention must be between zero and 1,000 years; the builder and the `retain.*` overrides
  refuse anything else (#39).
- `pgtasks.shutdown-timeout` defaults to 20 s, down from 30 s, so a stop fits in Kubernetes'
  default grace period. Keep it about 10 s below the platform's (#47).
- SPI: `TaskStore.registerNode(transaction, name, version, expiry)`. The three-argument form
  registers a node without an expiry of its own (#41).

### Fixed

- A retention that failed to purge for one topic stopped the purge for every topic after it. A
  retention the store can't read is now skipped with a warning (#39).
- A node whose clock ran ahead of the database's kept the store clock ahead until the database
  caught up. The first sample from the database now sets it (#40).
- The housekeeping leader requeued every node's tasks by its own `node-expiry`, so a node with a
  longer one lost its running tasks while it was alive. Each node now records its expiry when it
  registers (#41).
- A node that went silent during a claim, a lease renewal or a non-`APPEND` push kept its locks
  until TCP keepalive gave up, hours by default, which stalled `SINGULAR_*` topics and other
  nodes' heartbeats. Those transactions now set `idle_in_transaction_session_timeout` to
  1 minute, and the store's own transactions set a 5-minute network timeout (#42).
- A completion that failed for a reason the engine doesn't retry failed every task in the batch.
  The engine now fails only the tasks whose downstream work can't be written. Statement and lock
  timeouts, insufficient resources and read-only errors get three retries (#43).
- A `SUPERSEDE` or `REPLACE` push took one advisory lock per identifier, so a large one could
  run the server out of lock table space. Past 256 distinct identifiers it takes one lock for
  the topic (#45).
- A batch past its `batchDeadline` failed every task, including those already decided. Decisions
  made before the deadline now stand, tasks still being decided fail, and tasks whose decider
  never started go back without counting an attempt (#46).
- A stop waited for each topic's cancelled handlers in turn, so every slow topic added to the
  time past the timeout. Now all topics share one wait (#47).
- Outputs, filter reasons and failure messages were stored whole, on every retry. The engine
  keeps the first 65,536 characters of an output or reason and 8,000 of a message, with a note
  of how many more there were (#48).

## [0.1.0]

The first release: a rewrite of [Skatteetaten's task library](https://github.com/Skatteetaten/skatteprosessen-task)
(1.x) for Java 25 and PostgreSQL 17 to 19.

### Added

- Pushing tasks in the application's own transaction: `tasks.push(...)` joins the current
  transaction, `tasks.on(connection)` an explicit JDBC one, and `tasks.independently()` commits
  on its own. Workers are woken after the commit through `LISTEN`/`NOTIFY`.
- Typed topics (`TopicRef`), per-task and batch handlers, and decisions: success, filtered,
  suspended, later and failed. Downstream pushes from a handler commit with its completion.
- Claiming batches with `SKIP LOCKED` across any number of nodes, with fenced completions, so a
  node that lost its claim can't complete the task.
- Orderings (`FIFO` and three `SINGULAR_BY_IDENTIFIER` variants) and insertions (`APPEND`,
  `SUPERSEDE`, `REPLACE`).
- Retries with backoff (10 s doubling to 30 minutes, for 7 days by default), dead letters, and
  operator actions to revive or discard them. A dead letter is in the queue state `dead_letter`
  (`QueueState.DEAD_LETTER`, `QueueCounts.deadLetters()`, the `deadLetters` count in the endpoint
  and `state="dead_letter"` in metrics); `failed` is only ever an outcome, of a finished task.
- Releasing suspended tasks: `admin().release(...)` by id or `TaskQuery`, the endpoint's
  `release` action and the dashboard make them ready again, keeping their attempts.
- Housekeeping on one leader at a time: requeueing the tasks of dead nodes, queue snapshots and
  retention per outcome.
- `pgtasks-postgres`: the schema as plain SQL with a built-in runner (or Flyway and Liquibase),
  a configurable schema name, a validate-only mode, and `LegacyImport` to move queued 1.x tasks.
- `pgtasks-memory`: an in-memory store that passes the same TCK as Postgres. Given a
  `DataSource`, it runs batch resources' JDBC effects on it, so handlers that write SQL in
  `beforeCommit` can be tested with the tasks in memory (`TaskEngineExtension.withDataSource`,
  and the starter's application `DataSource` under `pgtasks.store=memory`).
- `pgtasks-spring-boot-starter`: one engine from every `TaskConfigurer` bean, `pgtasks.*`
  overrides that fail startup on unknown topics or settings, Spring transactions, an Actuator
  endpoint, a health indicator and an optional web dashboard.
- `pgtasks-micrometer`: metrics, and tracing that follows a task from the push through every
  topic downstream.
- `pgtasks-jackson`: JSON payloads with Jackson 3.
- `pgtasks-junit` and `TaskTester`: an engine per test on the in-memory store with a manual clock.
- `pgtasks-test`: the store TCK, and a real Postgres per test run without Docker.
- Extensions: housekeeping jobs, notification channels, typed topic settings, claim gates, batch
  listeners, handler decorators, observers and store capabilities.
- Java modules: the core jars have a `module-info`, so only the public API, SPI and extension
  packages are exported. They also run on the classpath, as the Spring starter and
  `pgtasks-test` do.
- `pgtasks-rate-limit`: `RateLimit`, a token bucket per topic on each node, and
  `ClusterRateLimit`, a sliding window shared by every node. `pgtasks-rate-limit-postgres` keeps
  that window in its own table, applied with the store's schema mode through `ExtensionSchema`,
  which lets any extension ship its own SQL files.
- `pgtasks-bom` to import every module at one version.

[Unreleased]: https://github.com/khopland/pgtasks/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/khopland/pgtasks/releases/tag/v0.1.0
