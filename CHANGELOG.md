# Changelog

What changes in each release of pgtasks. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, a minor version may
break the API; the schema only ever moves forward, with migrations the store applies itself.

## [Unreleased]

### Added

- Cancelling tasks: `admin().cancel(...)` by id or `TaskQuery`, the endpoint's `cancel` action and
  the dashboard take unfinished tasks out of the queue, so they never run. Ready, delayed,
  retrying, suspended and dead-lettered tasks finish with the new outcome `cancelled`
  (`Outcome.CANCELLED`), kept 30 days by default (`retain.cancelled`); an active task is left to
  finish. Tasks a cancelled one held back under an ordering run next.

### Changed

- Schema version 2 (`V002__cancelled_outcome.sql`) adds the `cancelled` outcome. The store applies
  it on start; with schema mode `validate`, apply it with your migration tool first. Nodes of 0.1.0
  keep running against it during a rolling deploy.
- `TaskStore` has a new method, `cancel`. A store of your own must implement it, and pass the
  `OperatorTck` cases for it.

## [0.1.0] - 2026-11-27

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
  `SUPERSEDE`, `REPLACE`). A `SUPERSEDE` or `REPLACE` push of more than 256 distinct
  identifiers takes one lock for the topic rather than one per identifier.
- Retries with backoff (10 s doubling to 30 minutes, for 7 days by default), dead letters, and
  operator actions to revive or discard them. A dead letter is in the queue state `dead_letter`
  (`QueueState.DEAD_LETTER`, `QueueCounts.deadLetters()`, the `deadLetters` count in the endpoint
  and `state="dead_letter"` in metrics); `failed` is only ever an outcome, of a finished task.
- Releasing suspended tasks: `admin().release(...)` by id or `TaskQuery`, the endpoint's
  `release` action and the dashboard make them ready again, keeping their attempts.
- Housekeeping on one leader at a time: requeueing the tasks of dead nodes, queue snapshots and
  retention per outcome. A retention the store can't read is skipped with a warning and the
  other topics are still purged.
- Limits checked up front: identifiers, references and dedup keys take at most 2,048 bytes in
  UTF-8 (`Creation.MAX_KEY_BYTES`) and a longer one is refused when it's pushed; retention is
  between zero and 1,000 years. The engine stores the first 65,536 characters of an output or
  filter reason and 8,000 of a failure message, with a note of how many more there were.
- Failures stay with the tasks that caused them. A completion the database refuses fails only
  the tasks whose downstream work can't be written; statement and lock timeouts, insufficient
  resources and read-only errors get three retries. A batch past its `batchDeadline` keeps the
  decisions made before it, fails the tasks still being decided, and hands back the tasks whose
  decider never started without counting an attempt.
- Silent nodes don't hold locks: claims, lease renewals and non-`APPEND` pushes set
  `idle_in_transaction_session_timeout` to 1 minute, and the store's own transactions a
  5-minute network timeout. Each node registers its own `node-expiry`, and the housekeeping
  leader requeues a dead node's tasks by it.
- `pgtasks.shutdown-timeout` defaults to 20 s, so a stop fits in Kubernetes' default grace
  period. Keep it about 10 s below the platform's.
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
