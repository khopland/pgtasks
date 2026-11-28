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
- `pgtasks-circuit-breaker`: a per-node circuit breaker, as an extension. A topic whose recent tasks
  mostly fail (80 % of the last 50, from at least 5 tasks) stops claiming on that node and sends
  one probe task every 30 s, whose failure doesn't use up an attempt; the probe's success closes
  it. Failures that are the task's own fault (`deadLetterAt`, an undecodable payload, no attempts
  left) don't count. Settings `breaker.enabled`, `breaker.opens-at`, `breaker.window`,
  `breaker.min-tasks` and `breaker.probe-every`. The endpoint and dashboard show the open breakers
  (view `breaker`) and close one on an operator's word.
- Extensions can show operators what they know and let them act on it: an `OperatorView` (a
  table with actions on its rows, and a status and actions per topic) shows in the endpoint
  (`GET /actuator/tasks/extensions/{view}`, `POST .../{view}/{action}`, and on each topic) and the
  dashboard, without the starter knowing the extension.
- Engine-wide extension settings: `Extension.engineSettings()`, set with `TaskEngineBuilder.set(key, value)`,
  overridden as `extensions.<name>` (Spring: `pgtasks.extensions.<name>`), read with
  `context.setting(key)` and listed by `engine.engineSettings()`.
- More for extensions to work with: `context.transact(...)`, `tasks(transaction)` to push in the
  extension's own transaction, `admin()`, `topics()`, `wake(topic)`, probe permits
  (`Permit.probe`, whose counted failures don't use up an attempt), `Extension.of(name, install)`
  and `engine.extension(Type.class)`.
- `context.job(...)` returns the `Job`: `runsHere()` says whether this node runs it now, and
  `runSoon()` runs it now besides its schedule, on the node that holds a leader job's lease even
  when asked on another, told through the store's notifications. `context.channel(...)` returns
  the `Channel`, to `send` messages on.
- A claim gate's wait can say why, `Permit.waitFor(delay, "paused")`: the health, the endpoint and
  the dashboard show the reason with the held topic.
- `OperatorView.topics(names)`: what a view says about many topics, asked once per request, so a
  view that reads its state from the store reads it in one go.
- A batch listener's `Completed` says whose fault a failure was (`Failure.FAILED` or `TASK_FAULT`),
  and carries the error and the batch's duration; a gate's `claimed` names the tasks it let through.
- Seeing what extensions do: `EngineHealth.held()` (and the health indicator, the endpoint, the
  dashboard and the `pgtasks.claims.held` metric) shows topics an extension's claim gate holds
  back: at once for a wait with a reason, and once the topic has gone a poll interval without a
  claim for one without, so a rate limit spacing claims out doesn't show.
  `EngineHealth.jobs()` shows when each job last ran and failed. Observers are told of every wait
  with `TaskObserver.held`.
- `SettingKey.checking(...)`: a check every value of a setting passes, whether it comes from
  the default, code or an override; checks add up. The rate limits use it.
- For extension stores: `StoreTransaction.onRollback(undo)`, `TaskStore.shared(type, create)` for
  state kept in memory per store, and `ExtensionSchemaReleases` for an extension's schema upgrade
  test. The release snapshots every `*SchemaSnapshot` test class.
- `TaskTester.advance` runs the extensions' every-node jobs too, and `drain` moves the clock past
  a claim gate's wait.

### Changed

- An extension's `onStart` actions run before its engine's channel reloads, jobs and topics do, so
  nothing is claimed before it is ready, and one that fails the start leaves no node behind. A
  message on a channel that arrives meanwhile waits for them. They take a `Callback`, which may
  throw checked exceptions.
- An extension's `onStop` actions run last, once the topics, reloads and jobs have stopped, in the
  reverse order the extensions were installed, and only for extensions whose start actions all ran.
- A leader job of an extension runs under a lease of its own, named after its extension and
  itself (`<extension>/<job>`), among the nodes that have it, instead of only on the node running
  the engine's housekeeping, which might not have the extension. `context.leading()` is gone; ask
  the job's `runsHere()`.
- `context.capability(type)` makes each capability once per store, so engines on one store share
  it; it made a new one on every call.
- A value set in code for an extension's setting goes through the key's check, like an
  override: `options.set(RateLimit.PER_SECOND, Double.NaN)` fails the build instead of making
  the topic poll without pause.
- `ClaimGate.permit(topic, wanted)` is the method to implement, and `claimed` gets the claimed
  task ids instead of a count. `BatchListener.Completed` has more fields; its four-field
  constructor stays, for tests, with `withFailure` and `withDuration`.
- `ClaimGate.Permit`, `BatchListener.Completed`, `SettingKey` and `EngineHealth` are final classes
  instead of records, so they can grow without breaking again: the same accessors, and the
  factories and public constructors they had. Two `SettingKey`s are equal when their name, type
  and default are, and the declared key's check applies to a value set with another.
- The interfaces only the engine implements (`ExtensionContext`, its `Job` and `Channel`) get new
  methods in minor releases; the API check allows that for them.
- Hooks are taken only while an extension installs, and two extensions with the same `name()`
  can't be installed in one engine.
- Channel reloads are told the messages' payloads: `channel(name, payloads -> ...)`.
- `TaskStore` has a new method, `shared`, and `StoreTransaction` a new default one, `onRollback`.

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
