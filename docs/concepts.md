# Concepts

## Topics

A topic is a queue of tasks of one payload type, processed by one handler. Refer to it with a
typed `TopicRef`, defined once by the module that owns it:

```java
public static final TopicRef<AssessmentRequest> ASSESSMENT = TopicRef.of("assessment", AssessmentRequest.class);
```

Names use lower-case letters, digits, `.`, `_` and `-`. A topic is a row in `pgtasks.topic`, so
creating one needs no DDL. A topic is created when an engine that registers it starts, or that
declares it with `pushesTo` (see [producers in another service](patterns.md#producers-and-consumers-in-separate-services)),
or with `admin().createTopic(name)`. A push never creates one, so a misspelt topic fails at once.

Configurers register topics, one per module:

```java
TaskConfigurer notifyTasks = tasks -> tasks
    .defaults(options -> options.concurrency(4))           // this configurer's topics only
    .eachTask(NOTIFY, task -> mailer.send(task.payload()))
    .topic(REMINDERS, new ReminderHandler(), options -> options.ordering(Ordering.SINGULAR_BY_IDENTIFIER))
    .topicsMatching("assessment-*", AssessmentRequest.class, assessments) // also topics created later, by anyone
    .pushesTo(ARCHIVE);                                    // another module's topic: created at start, codec checked
```

All configurers are merged and validated before anything starts, so bean order doesn't matter.
Two configurers registering one topic, an unknown limit, an override for a topic nobody defines,
or a payload type without a codec all fail the build with a message that names the module.

## Tasks and identifiers

A task has an identifier: the application's key, such as a person or a tax return. Ordering and
insertion policies work per identifier within a topic. A push can also give a delay or a run
time, a suspension, a priority, a reference for correlation, a dedup key, and headers:

```java
tasks.push(ASSESSMENT, Insertion.SUPERSEDE, Creation.of(returnId, request)
    .withDelay(Duration.ofMinutes(5))
    .withPriority(10)
    .withDedupKey("assess:" + returnId + ":" + version)
    .withHeader("source", "self-service"));
```

Headers are names and values stored with the task and given back on `task.headers()` and
`task.header(name)`. Observers add their own when the task is pushed: the tracer's propagation
headers, and the MDC keys named in `pgtasks.mdc-headers`, such as a correlation id (see
[operations](operations.md#tracing-and-correlation)). A creation's own headers win over theirs.
The reference is one indexed value to look a task up by; headers are context that travels with
it.

`withDelay(duration)` waits that long from the push; `withRunAt(instant)` waits until a moment,
such as the start of the next business day. The run time is compared with the store's clock (the
database's, on Postgres), so nodes with skewed clocks agree on it, and a time that has already
come, the past included, makes the task ready at once. Delay, run time and suspension exclude each
other: each of the three builders replaces the others. Downstream work pushed from a handler
(`work.push`) takes the same creation. Recurring tasks don't take one: their cron is the time.

`push` returns one `Pushed` per creation: `QUEUED`, `REDUNDANT` (a later creation in the same
push has the same identifier), `DUPLICATE` (an unfinished task has the dedup key), or `PENDING`
(the in-memory store applies it once the transaction commits).

### Insertion

What a push does to earlier tasks with the same identifier in the topic. A task a worker has
already claimed is never touched.

| Insertion | Earlier tasks waiting to run | Suspended and dead letters | Finished history |
|---|---|---|---|
| `APPEND` (default) | kept | kept | kept |
| `SUPERSEDE` | become redundant | kept | kept |
| `REPLACE` | become redundant | become redundant | marked redundant, outcome kept in `previous_outcome` |
| `DELETE` | deleted | deleted | deleted |

Insertion is a queue-management tool, not a uniqueness guarantee. Use a dedup key for that.

Earlier and later mean task-id order. A push draws its ids before it commits, so two pushes for one
identifier could commit out of id order, and the one that commits first couldn't see the other's
task. So every push but `APPEND` waits per identifier for another uncommitted non-`APPEND` push of
the same identifier to finish, and its ids follow commit order (in read committed transactions,
the Postgres default). A transaction holds these waits until it commits: one that pushes several
identifiers in separate calls should push them in the same order as other producers, or in one
call, to avoid deadlocks. `APPEND` waits for nothing, since it is the common, cheapest push, so
concurrent `APPEND` pushes for one identifier may commit out of id order.

### Ordering

`FIFO` (default) runs by priority, then by push order, with no per-identifier constraint.
The `SINGULAR_BY_IDENTIFIER*` orderings never run two tasks for one identifier at once, and make
a later task wait for an earlier one:

| The earlier task is… | `SINGULAR_BY_IDENTIFIER` | `…_ON_FAILURE` | `…_UNTIL_SUCCESS` |
|---|---|---|---|
| ready, active, retrying, throttled | waits | waits | waits |
| suspended or delayed | runs | runs | waits |
| dead-lettered | runs | waits | waits |
| finished | runs | runs | runs |

"Earlier" is task-id order. When concurrent `APPEND` pushes for one identifier commit out of id
order, the later task may start first; the earlier one then waits until it is done rather than run
beside it. Claims under a `SINGULAR_…` ordering take turns per topic until they commit, so a claim
always sees what the previous one took; `FIFO` claims don't wait for each other.

## Handlers and decisions

The common case is a `TaskHandler`: it succeeds when it returns and fails when it throws.

```java
tasks.eachTask(NOTIFY, task -> mailer.send(task.payload()));
```

A `BatchHandler` sees the whole batch, for one lookup per batch or for decisions other than
success and failure:

```java
tasks.topic(ASSESSMENT, batch -> {
    Parties parties = register.lookup(batch.payloads());             // once per batch
    return batch.decide(task -> {
        if (!parties.has(task.payload().person())) {
            return Decision.later(Duration.ofHours(1));
        }
        task.push(NOTIFY, task.identifier(), notice(task.payload(), parties));
        return Decision.success();
    });
});
```

`batch.decideConcurrently(n, decider)` decides up to `n` tasks at once on virtual threads, for
deciders that wait on a remote call per task.

| Decision | What happens | Counts an attempt |
|---|---|---|
| `success()`, `success(output)` | Done. Staged downstream work is committed. | — |
| `filtered(reason)` | Done, not needed. Nothing downstream. | — |
| `later(delay)` | Runs again after the delay. | no |
| `suspended()` | Waits until a junction or an operator releases it. | no |
| `throttled(retryAfter)` | Downstream pushback: retried after the delay. | no |
| `unavailable(cause)` | Not the task's fault: retried with backoff, error kept. | no |
| `failed(cause)` | Retried with backoff, or dead-lettered when the policy gives up. | yes |

An exception from a decider fails only that task. One from the handler fails the batch. A task
the handler leaves without a decision fails too.

Downstream work goes through `task.push(...)` on the task, not through `Tasks`. It is committed
together with the task's completion, and only if the task succeeds. A handler that calls
`tasks.push(...)` gets an error saying so, or a warning if it runs in a transaction of its own.

## Retries and dead letters

The topic's `Retry` decides what a counted failure means:

```java
Retry.standard()                                                       // 10 s doubling to 30 min, for 7 days after the first failure
Retry.exponential(Duration.ofSeconds(5), Duration.ofMinutes(10)).giveUpAfter(Duration.ofDays(1))
Retry.standard().attempts(5)                                           // a number of attempts instead of a time budget
Retry.standard().forever()                                             // never gives up
Retry.none()                                                           // dead letter at the first failure
Retry.standard()
    .notCounting(ConnectException.class, SocketTimeoutException.class) // the system's fault: retried, attempt not counted
    .deadLetterAt(ValidationException.class)                           // the task's fault: dead letter at once
```

The time budget counts from the task's first failure. Backoffs have 20 % jitter. A payload the
codec can't decode is always dead-lettered at once, since retrying won't change it.

A dead letter stays in the queue, holding back later tasks under `…_ON_FAILURE` and
`…_UNTIL_SUCCESS`, until an operator revives it (another round of attempts) or discards it
(finished as failed). A topic can also revive them by itself:
`options.deadLetters(DeadLetterPolicy.KEEP.withReviveOnNewVersion())` revives them when the first
node of a new application version starts, so a bug fix gets a second go at what it broke.

A task whose node dies under it is requeued with backoff once the node counts as dead (10
minutes without a heartbeat by default). The third time, it is dead-lettered whatever the
policy says, so a task that crashes the JVM can't take down node after node.

## Operator actions on unfinished tasks

Operators can also act on tasks that are still waiting, not only on dead letters
(`engine.admin()`, the endpoint and the dashboard; see [operations](operations.md#unfinished-tasks)):

- **Search** a topic's unfinished tasks by identifier, reference, state, one header's value and
  when they were pushed, a page at a time, lowest id first.
- **Run now** makes delayed and retrying tasks ready at once. Their attempts and failures stay, so
  a further failure backs off as it would have.
- **Cancel** takes ready, delayed, retrying, suspended and dead-lettered tasks out of the queue.
  They finish with the outcome `cancelled` and never run; like a discarded dead letter, a
  cancelled task stops holding back later tasks under a `SINGULAR_…` ordering. An **active** task
  is never cancelled: its node is running it and can't be stopped, so the cancel skips it and it
  runs to its end with its own outcome. Cancel it again if it comes back to wait.

`cancelled` is an outcome of its own rather than `failed` (which says the task gave up) or
`redundant` (which says a newer push replaced it): it says an operator decided, and it has its own
retention, 30 days by default. A cancelled task still counts as present for an `ANY` junction,
like any task that isn't redundant, and `REPLACE` leaves it in the history as it is.

## The breaker

When most of a topic's recent tasks fail (80 % of the last 50, from at least 5 different tasks),
a node stops claiming from it and sends one probe task every 30 s. The probe's failures don't
count, and the first probe that succeeds closes the breaker. Failures the policy dead-letters at
once (`deadLetterAt`) don't count, since bad input isn't an outage. Change it with
`options.breaker(Breaker.standard().opensAt(0.5, 20))`, or turn it off with `Breaker.off()`.

## Rate limits

A limit caps how many tasks are claimed per second across every node, with one token bucket in
the database. Define it once and use it from any topic:

```java
TaskGlobals globals = g -> g
    .limit("folkeregister", limit -> limit.rate(Rate.perSecond(200)).burst(20)
        .window(Cron.parse("0 7 * * MON-FRI", oslo), Rate.perSecond(50))     // office hours: gentler
        .window(Cron.parse("0 17 * * MON-FRI", oslo), Rate.perSecond(200)));
...
options.limit("folkeregister");
```

Operators override a limit at runtime through the endpoint or `engine.limits().override(...)`,
optionally for a while. Every node picks it up within about a second.

## Recurring tasks

```java
tasks.recurring(REPORTS, "nightly", Cron.parse("0 2 * * *", oslo), "nightly-report", new ReportRequest());
```

The housekeeping leader pushes it when due, once across the cluster. A run missed while no node
was up is made up once. By default each push supersedes the last one if that is still waiting.

## Where settings come from

Each layer overrides the one before it:

1. Built-in defaults: 100 tasks per batch, 2 batches per topic per node, FIFO, the standard
   retry, a 30 minute batch deadline, a 30 s poll as a fallback for notifications. Succeeded
   tasks are kept for 30 days, cancelled ones too; filtered, redundant and recreated for 7; failed forever.
2. `TaskGlobals.defaults(...)`, for every topic.
3. A configurer's `defaults(...)`, for its own topics.
4. The topic's own options.
5. Overrides: `pgtasks.topics.<topic>.<setting>` in Spring, or `builder.overrides(map)`. The keys
   are `concurrency`, `batch-size`, `ordering`, `poll-interval`, `batch-deadline`, `breaker`
   (on or off), `retry.max-attempts`, `retry.give-up-after`, `retry.first-backoff`,
   `retry.max-backoff` and `retain.<outcome>`. Limits take `limits.<limit>.rate` and `.burst`.

`engine.effectiveSettings()`, and the endpoint, show each topic's values and the layer each one
came from.
