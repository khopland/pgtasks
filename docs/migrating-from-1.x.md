# Migrating from 1.x

pgtasks keeps what 1.x does well: transactional enqueue, the insertion policies and ordering per
identifier. Most of what applications wrote around 1.x is now in the library:
housekeeping jobs, retries, reviving, metrics and wiring. It runs on Postgres only, in its own
`pgtasks` schema, and needs Java 25.

## What maps to what

| 1.x | 2.0 |
|---|---|
| `TaskManager` with its 12 constructor arguments, one per topic | `TaskEngine.builder()`, or the Spring starter, with one `TaskConfigurer` per module |
| Your own heartbeat, expire, scheduled, purge and purgeOwners jobs | Built in: one node leads housekeeping by a lease, retention per topic and outcome |
| `INPUT` and `OUTPUT` as strings | A typed payload with a `PayloadCodec`, or JSON by default with Jackson; `Decision.success(output)` |
| `TaskSink.Insertion` APPEND, SUPERSEDE, REPLACE, DELETE | `Insertion` APPEND, SUPERSEDE, REPLACE. DELETE is not in this version: use REPLACE, and retention for the history |
| `TaskSource.Condition` NONE, SINGULAR_BY_IDENTIFIER, …_SUSPEND_ON_FAILURE, …_SUSPEND_UNTIL_SUCCESS | `Ordering` FIFO, SINGULAR_BY_IDENTIFIER, …_ON_FAILURE, …_UNTIL_SUCCESS, now applied within the topic only, and always |
| `TaskSource.Order` LAST_IN_FIRST_OUT | Dropped. Use `Creation.withPriority(n)` for work that should jump the queue |
| `TaskJuncture` SINGULAR, ETERNAL | Not in this version. The waiting task checks its parts itself and decides `later(delay)` until they are done ([patterns](patterns.md#fan-out-and-fan-in)) |
| Results SUCCESS, FILTER, SUSPENSION, FAILURE | `Decision.success()`, `filtered(reason)`, `suspended()` or `later(delay)`, `failed(cause)` |
| FAILED stays failed until `TaskReviver` resets it | A retry policy, then dead letters, revived by an operator by id or error type |
| EXPIRED tasks from dead owners are never requeued | Requeued with backoff once the owner counts as dead; dead-lettered the third time |
| Contexts and `TaskSupplement` lookups | `BatchResource` (open, review, beforeCommit, afterCommit) and `batch.resource(...)` |
| `TASK_PRETTY` | `pgtasks.task_view` |
| `TaskLimiter` | Concurrency per topic and the node-wide in-flight cap. `pgtasks-rate-limit` adds a per-node, per-topic rate limit; cluster-wide limits are not in this version |

Behaviour that changes:

- Attempts are bounded. A topic that relied on "reset failed tasks every 30 minutes, forever" can
  keep that with `Retry.standard().forever()`.
- Retention counts from completion, not from creation.
- A scheduling delay on a successful or filtered task never makes it run again. Work that
  repeats pushes its next run itself, with a delay and `SUPERSEDE`.
- Identifier conditions look only at the same topic. In 1.x, a task in another topic with the same
  identifier could block one (finding #6 of the plan).
- Children are never committed for a parent that didn't continue, whichever context or resource
  stopped it (finding #8).

## Moving queued tasks

`LegacyImport` (in `pgtasks-postgres`) copies a 1.x `TASK` table in the same database into the
`pgtasks` schema.

1. Stop every 1.x node, so nothing claims or completes while you copy.
2. Apply the `pgtasks` schema: start a 2.0 engine once with `schema-mode=migrate`, or run
   `TaskSchema.migrate(dataSource)`, or your Flyway or Liquibase setup. If the engine uses another
   schema name (`pgtasks.schema`), pass the same name to `LegacyImport.Options.withSchema(...)`.
3. Run the import:

   ```java
   LegacyImport.Report report = LegacyImport.run(dataSource, LegacyImport.Options.defaults());
   // .withHistory() to copy finished tasks too; .withTopicNames(name -> ...) to rename topics;
   // .withTable("legacy.task") if the table isn't on the search path
   ```

4. Start the 2.0 application. Each topic needs a codec that reads what 1.x stored in `INPUT`.
   A topic of strings reads it directly. Otherwise, write a codec for the 1.x format, or convert
   the payloads with a small job first.

What it does:

- Topic names are lower-cased by default. A name that isn't a valid topic name, or two topics
  that would get the same name, stop the import before anything is copied.
- READY, ACTIVE and EXPIRED tasks become ready, or delayed until their `SCHEDULED` time if it is
  still to come. SUSPENDED tasks stay suspended. FAILED tasks become dead letters, with the error
  type `task1.failed`.
- Tasks keep their sequence order, so ordering by identifier carries on as it was.
- Each topic is copied in one transaction. A topic that already has 2.0 tasks is skipped and
  reported, so running the import again does no harm. The 1.x table is only read.

## Coordinates

pgtasks has its own group (`io.github.khopland.pgtasks`), packages and artifact names (`pgtasks-*`), so
it can run beside 1.x in one application while you move topic by topic: each library works in
its own tables.
