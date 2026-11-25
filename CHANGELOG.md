# Changelog

What changes in each release of pgtasks. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/). Before 1.0, a minor version may
break the API; the schema only ever moves forward, with migrations the store applies itself.

## [Unreleased]

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
  operator actions to revive or discard them.
- Releasing suspended tasks: `admin().release(...)` by id or `TaskQuery`, the endpoint's
  `release` action and the dashboard make them ready again, keeping their attempts.
- Housekeeping on one leader at a time: requeueing the tasks of dead nodes, queue snapshots and
  retention per outcome.
- `pgtasks-postgres`: the schema as plain SQL with a built-in runner (or Flyway and Liquibase),
  a configurable schema name, a validate-only mode, and `LegacyImport` to move queued 1.x tasks.
- `pgtasks-memory`: an in-memory store that passes the same TCK as Postgres.
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
