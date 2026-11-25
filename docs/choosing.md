# When to use pgtasks, and when not to

pgtasks is a task queue for Java applications that already keep their data in Postgres. It is
built around one idea: the task is a row in the same database as the data change it reacts to, so
it commits or rolls back with that change. If that is the problem you have, it is probably a good
fit. If it isn't, one of the tools below is likely a better one.

## A good fit

- **Work that must follow a data change, exactly when that change commits.** A signup and its
  welcome email, an order and its payment, an import and its post-processing. With pgtasks the
  push joins the application's transaction (Spring's, or a JDBC connection's), so there is no
  outbox table to poll, no two-phase commit, and no window in which the data committed but the
  message was lost, or the message went out for data that rolled back.
- **Steps that must happen exactly once in effect.** A handler runs at least once, as in every
  queue. What it writes to the database and the tasks it pushes commit with its completion, and a
  completion from a node that has lost its claim is refused, so those effects happen once even
  when the handler runs twice. Effects outside the database get the task's idempotency key. See
  [Patterns](patterns.md#what-runs-once-and-what-may-run-twice).
- **Ordering per business key.** `SINGULAR_BY_IDENTIFIER` and its variants run one task at a
  time per identifier (a customer, an account) while different identifiers run in parallel, and
  `SUPERSEDE` and `REPLACE` let a newer task make older queued ones redundant.
- **Teams that would rather not run more infrastructure.** No broker, no separate scheduler
  service: the queue is a schema in the database you already back up, monitor and fail over.
  Operators get an Actuator endpoint, a dashboard, metrics and health without another system.
- **Moderate throughput.** On the benchmarks' Linux box with durable commits, one task per transaction,
  the producers run out before pgtasks does at about 3,000 to 3,500 tasks a second; with 100
  tasks per transaction three nodes handle 17,000 to 18,000 a second
  ([benchmarks](benchmarks/2026-11-22-notify-and-throughput.md)). That is far above what most
  business workflows need, and below what an event stream can do.

## Not a good fit

- **Event streaming, fan-out to many independent consumers, or replay.** A task is claimed by one
  worker and then it is done. If many services each need their own copy of every event, need to
  re-read history, or need hundreds of thousands of events a second, use Kafka or another log.
  pgtasks can sit on either side of one: [Patterns](patterns.md#producers-and-consumers-in-separate-services).
- **A database other than Postgres 17 or later.** pgtasks uses `SKIP LOCKED`, `LISTEN`/`NOTIFY`
  and Postgres-specific SQL throughout, and refuses older servers.
- **Java before 25.** The engine uses scoped values, which became final in Java 25, and virtual
  threads.
- **Large payloads.** Payloads are stored as text in the task table, and kept for the retention
  period. Store files and large documents elsewhere and push a reference.
- **Cron schedules, today.** Delays and run times are supported (`withDelay`, `withRunAt`,
  `later()`), and a task can push its own next run, but there is no built-in "every night at
  02:00 on one node". It is planned for 0.2.0, as is cancelling a queued task by id.
- **Work that has nothing to do with the database's transactions.** If the work never needs to
  commit with a data change, an in-memory executor or a scheduler may be simpler.

## Compared with other tools

These are short, and fair to the best of our knowledge; check each project's own documentation
before deciding.

| | pgtasks | db-scheduler | JobRunr | Quartz | A message broker |
|---|---|---|---|---|---|
| What it is | Task queue in Postgres | Persistent, cluster-friendly task scheduler | Background job library with a dashboard | Job scheduler with cron triggers | A separate system that moves messages |
| Databases | Postgres 17+ | Many (Postgres, MySQL, SQL Server, Oracle, ...) | Many SQL and NoSQL stores | Many, through JDBC | Its own storage |
| Push in the app's transaction | Yes, the default | Possible, with care (see its transactional staging example) | Pro edition (transaction plugin) | With a container-managed (JTA) job store | No: needs an outbox |
| A run's database effects commit with its completion | Yes, fenced against stale nodes | Not built in | Not built in | Not built in | Not built in |
| Ordering per key | Yes | Not built in | Not built in | Not built in | Per partition (Kafka), per queue |
| Recurring / cron | Not yet (0.2.0) | Yes | Yes | Yes, its core strength | No |
| Dashboard | Yes, with operator actions | Community add-on | Yes; search and more in Pro | No | Product-specific |
| Retries with backoff, dead letters | Yes | Yes, configurable failure handlers | Yes; custom policies in Pro | Up to the job | Product-specific |
| Rate limits | Per node and cluster-wide (`pgtasks-rate-limit`) | Not built in | Pro edition | Not built in | Product-specific |
| License | Apache 2.0 | Apache 2.0 | LGPL core; commercial Pro | Apache 2.0 | Varies |

In short:

- **db-scheduler** is the closest relative: a Java library that keeps its work in your database.
  Choose it for recurring tasks, or if you aren't on Postgres. Choose pgtasks for transactional
  pushes as the default, completions that commit the run's effects, per-key ordering and the
  operator tooling.
- **JobRunr** schedules Java lambdas, with a polished dashboard and many storage backends. Several
  features that pgtasks has in its open-source core (rate limits, transactional enqueueing,
  custom retry policies) are in JobRunr's commercial Pro edition.
- **Quartz** is a scheduler first: cron triggers, calendars, misfire handling. It is not a queue,
  and its clustering is about running each trigger on one node.
- **A broker** (Kafka, RabbitMQ, SQS) is the right tool for streams, many consumers and very high
  volume. Making its messages consistent with your database takes an outbox, which is the problem
  pgtasks avoids by being the outbox.

## What pgtasks guarantees, and what it doesn't

- A pushed task is stored if and only if the pushing transaction commits.
- Each task runs until it succeeds, is filtered, waits (`later()`, suspended) or runs out of
  retries and becomes a dead letter that an operator can revive.
- A handler may run more than once: after a node dies mid-batch, after a timeout, after a failed
  completion. Database effects made through the completion, and pushes from the handler, are
  committed once. Anything else should use `task.idempotencyKey()`.
- A node that dies keeps its claims until it counts as dead (10 minutes by default, configurable),
  then another node requeues them.
- Throughput is bounded by Postgres's write capacity: every task is an insert, an update and
  later a delete.

The [soak test](../pgtasks-benchmark/src/test/java/io/github/khopland/pgtasks/benchmark/SoakBenchmark.java)
checks the first three under sustained load while nodes are killed, frozen, cut off from the
database and redeployed.
