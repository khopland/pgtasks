# Writing an extension

The engine's core is small: topics, pushing in the caller's transaction, claiming, retries, dead
letters and housekeeping. Features beyond that, such as a rate limit, a pause, a circuit breaker
or recurring tasks, belong in extensions. An extension uses the same hooks the engine's own
housekeeping uses, so it needs no change to the engine. Everything an extension implements or
receives is in the package `io.github.khopland.pgtasks.engine.extension`.

```java
public final class NightlyReport implements Extension {

    @Override
    public void install(ExtensionContext context) {
        context.job(HousekeepingJob.leader("nightly report", Duration.ofMinutes(1), () -> {
            // runs on one node at a time, among the nodes that have this extension
        }));
    }
}
```

Register it with `TaskEngine.builder().extension(new NightlyReport())`. With Spring, make it a
bean: the starter installs every `Extension` bean, in `@Order`. A one-hook extension can be a
lambda with a name: `Extension.of("audit", context -> context.batchListener(audit::record))`.

## Lifecycle

- **Installed once per engine**, when the engine is built: `install` gets a context of that
  engine's own. One instance may be installed in several engines (one per test, or the nodes of a
  `TaskTester` on one store), so keep what belongs to one engine in what `install` creates, not in
  the instance's fields. Two extensions with the same `name()` can't be installed in one engine:
  a feature installed twice usually counts twice. `TaskEngine.extensions()` lists the installed
  ones, and `TaskEngine.extension(Pause.class)` finds one.
- **Hooks are registered in `install`.** A hook registered after it returns throws. The rest of
  the context (the store, `transact`, `wake`, the settings) works for as long as the engine lives.
- **`onStart` runs before the topics and jobs**, each time the engine starts, restarts included.
  Ready what the gates and jobs use there (apply a schema, load state); one that throws fails the
  start before anything is claimed. `onStop` runs as the engine stops, before its topics do.

## The hooks

| Hook | What it does |
|---|---|
| `claimGate(ClaimGate)` | Asked before every claim on this node: claim at most `n` tasks (`Permit.atMost`), a probe of `n` (`Permit.probe`), or none for a while (`Permit.waitFor`). The strictest gate wins. `claimed(topic, taskIds)` tells it which tasks the claim got. Return `Permit.ANY` when nothing limits the claim. |
| `batchListener(BatchListener)` | Told what became of each task of a batch once its completion committed: its result, whether it failed and whose fault that was, the error, and how long the batch took. |
| `handlerDecorator(HandlerDecorator)` | Wraps the handler of every topic this node runs; the first registered is outermost. |
| `observer(TaskObserver)` | Sees this node's pushes and batches, for tracing, metrics, or headers that follow the work, next to the observers given to the builder. For a header name both give, the builder's value is stored. One that throws is logged and ignored. |
| `job(HousekeepingJob)` | Runs work on a fixed delay, on every node (`everyNode`) or on one node at a time (`leader`). |
| `channel(name, reload)` | Reloads something that may have changed on another node, told what the messages said. |
| `operatorView(OperatorView)` | Shows operators what the extension knows, and lets them act on it, in the starter's endpoint and dashboard. |
| `onStart` / `onStop` | Runs each time the engine starts (before its topics and jobs) and stops (before its topics stop). |

And what it can use:

- `store()`, `capability(Type.class)`: the engine's store, and the extension's own view of it (below).
- `transact(what, transaction -> ...)`: work in a transaction of its own, committed if it returns
  and rolled back if it throws, with failures unchecked.
- `tasks()`, and `tasks(transaction)` to push in the extension's own transaction, so a schedule
  that fires and the task it pushes commit together.
- `admin()`: the operator's side, such as every topic in the store; `topics()`: those this node registers.
- `clock()`: the store's clock, the database's on Postgres.
- `topicSetting(topic, key)` and `setting(key)`: the extension's per-topic and engine-wide settings.
- `leads(job)`: whether this node holds a leader job's lease now. A leader job that loops over
  batches stops when it turns false.
- `wake(topic)`: asks a topic's gates again now, for a gate whose answer changed.
- `runSoon(job)`: runs one of its jobs now, besides its schedule, such as for an operator's "run now".

## Claim gates

A gate is asked on the topic's poll thread before every claim, so it must be quick. A gate that
holds a topic back says how long; the topic asks again then, once a poll interval at the latest,
or at once when it is woken. While a gate holds a topic, the engine's health
(`EngineHealth.held()`), the endpoint, the dashboard and the `pgtasks.claims.held` metric show
it, under the name of the extension that registered the gate, and observers are told
(`TaskObserver.held`). A topic held back counts as having found nothing, so `TaskTester.awaitIdle`
sees it as idle, and `TaskTester.drain` moves the clock past the wait.

A gate that throws is skipped and the claim goes ahead, so a gate that must hold claims back when
its storage fails catches the error and returns a wait itself.

A **probe** (`Permit.probe(1)`) is a claim to find out whether the topic works again, as a circuit
breaker sends while it is open: a counted failure of a probed task doesn't use up an attempt.
`claimed` names the tasks the probe got, so the gate can tell the probe's result from those of
batches that were already running when it opened.

The engine tells each gate how many tasks the claim wants (`permit(topic, wanted)`), so a gate
that reserves a shared allowance takes no more than the batch size, and every granted permit is
settled once with `claimed`, with no ids when another gate held the claim back or the claim failed.

## Batch listeners

A listener gets each task of a batch after its completion committed (or its failure was
recorded): `Completed(taskId, identifier, attempt, result, failure, error, duration)`. `failure`
says whether it failed and whose fault that was: `FAILED` for a failure that says something about
what the handler talks to, `TASK_FAULT` for the task's own (a failure the retry policy
dead-letters at once, an undecodable payload, a task out of attempts). A breaker counts the first
and leaves the second out. Tasks handed back without running, as on a stop, aren't reported.

## Jobs

`HousekeepingJob.everyNode(name, interval, action)` runs on every node that has it;
`HousekeepingJob.leader(...)` on one node at a time, the one holding the job's own lease. That
lease is named after the job and only nodes with the job ask for it, so a leader job runs even
when another application, without the extension, shares the schema and runs the engine's
housekeeping. Leases show with each node in the endpoint and the dashboard. Leader jobs must be
safe to run twice at once for up to a heartbeat, while a hung leader finds out it lost the lease.

Job names are unique per engine and can't be the engine's own (`promotion`, `retention` and the
rest). Each extension job has a thread of its own. One that throws is logged and runs again after
its interval, and `EngineHealth.jobs()` shows when each job last ran and last failed, with what.
`TaskTester.advance` runs the leader jobs on the nodes that hold their leases and every node's
extension jobs, at the new time.

## Channels

`channel(name, payloads -> ...)` reloads something when it may have changed on another node. Send
a message with `store().notify(transaction, name, payload)` in the transaction that made the
change, so it arrives once that commits. The reload runs on a thread of its own, told the payloads
of the messages since its last run; an empty set means anything may have changed, and comes after
a lost notification connection, on every heartbeat (in case a message got lost), and when more
messages came in than are worth listing. Requests that arrive while a reload waits are served by
that one run. `channel(name, () -> ...)` is the same without the payloads.

## Settings

An extension declares settings with `SettingKey`: a dotted lowercase name, a type, a default, a
parser for overrides, and optionally a check that applies to every value, the default, values set
in code and overrides alike:

```java
static final SettingKey<Double> RATE = SettingKey.of("rate.per-second", Double.class, 0.0, Double::valueOf)
    .checking(rate -> {
        if (!Double.isFinite(rate) || rate < 0) {
            throw new IllegalArgumentException("must be finite and not negative, not " + rate);
        }
    });
```

**Per-topic** settings come from `settings()`. Topics set them like any option,
`options.set(RATE, 20.0)`, in defaults too; overrides and Spring YAML use the name:
`pgtasks.topics.assessment.rate.per-second: 50`. The value layers like the engine's own settings,
and `engine.effectiveSettings()` and the endpoint show it with its source.

**Engine-wide** settings, such as how often an extension looks for work, come from
`engineSettings()`. Code sets them on the builder, `TaskEngine.builder().set(INTERVAL, Duration.ofSeconds(5))`;
overrides use `extensions.<name>`, in Spring `pgtasks.extensions.recurring.interval: 5s`. The
extension reads them with `context.setting(INTERVAL)`, and `engine.engineSettings()` shows each
with its source. A setting or override no installed extension declares fails the build.

## Operator views

An `OperatorView` shows operators what the extension knows and lets them act on it, without the
starter knowing the extension. It has two parts, each optional:

- **a table**: `columns()` and `rows()`, each row with a key and the actions it offers, such as
  recurring tasks with "Run now";
- **what it says about a topic**: `topic(name)` returns a short status and the actions it offers
  on that topic, such as "paused" with "Resume", or "Pause" on a topic that isn't.

```java
context.operatorView(new OperatorView() {
    public String name() { return "pause"; }
    public String title() { return "Paused topics"; }
    public List<Column> columns() {
        return List.of(Column.of("topic", "Topic", Column.Kind.TOPIC), Column.of("since", "Paused", Column.Kind.TIME));
    }
    public List<Row> rows() {
        return paused().stream().map(p -> Row.of(p.topic(), Map.of("topic", p.topic(), "since", p.at()), "resume")).toList();
    }
    public TopicState topic(String topic) {
        return isPaused(topic) ? TopicState.of("paused", "resume") : TopicState.of(null, "pause");
    }
    public List<Action> actions() {
        return List.of(Action.of("pause", "Pause", "No node claims from it until it is resumed.", this::pause),
            Action.of("resume", "Resume", null, this::resume));
    }
});
```

The endpoint serves each view at `GET /actuator/tasks/extensions/{name}` and in the overview,
adds what each view says to every topic, and runs an action with
`POST /actuator/tasks/extensions/{view}/{action}` and `{"key": "..."}` for a row or
`{"topic": "orders"}` for a topic, only where the row or topic offers it. The dashboard draws the
table as a panel on the overview, the status next to the topic, and the topic's actions on its
page, asking the operator to confirm with the action's `confirm` text.

## Store capabilities

An extension that keeps state of its own needs store-specific code: statements on Postgres, maps
in memory. It ships an interface, say `WindowStore`, and a `StoreCapability` per store it
supports, registered in `META-INF/services/io.github.khopland.pgtasks.spi.StoreCapability`:

```java
public final class PostgresWindows implements StoreCapability {
    @Override
    public <C> Optional<C> create(Class<C> type, TaskStore store) {
        return type == WindowStore.class && store instanceof PostgresTaskStore postgres
            ? Optional.of(type.cast(new PostgresWindowStore(postgres))) : Optional.empty();
    }
}
```

The extension asks for it with `context.capability(WindowStore.class)`. Without a provider for the
engine's store, that fails with a message naming the missing module.

**In memory**, state must be shared by every engine on the store and roll back with its
transaction, as the store's own does. `store.shared(InMemoryWindows.class, InMemoryWindows::new)`
keeps one instance per store, and `transaction.onRollback(undo)` undoes a change if the
transaction, or a savepoint taken before it, rolls back.

**On Postgres**, an extension that needs a table ships its SQL in its own jar; it does not add files to the engine's
schema. `ExtensionSchema` names that classpath directory and the tables it expects:

```java
private static final ExtensionSchema WINDOWS = ExtensionSchema.of("windows", PostgresWindowStore.class,
    "io/example/windows/db", List.of("V001__windows.sql"), List.of("window"));
```

`WINDOWS.apply(postgres)` applies the files with the built-in runner when the store's schema mode
is `migrate`, checks the tables in `validate`, and does nothing in `none`. The runner keeps history
in `<schema>.windows_schema_history` under its own advisory lock, so version numbers and checksums
can't clash with the engine's or with another extension's. The files use the same
`${pgtasks_schema}` placeholder and `V<version>__...` / `R__...` rules as the engine's, and
applications using Flyway, Liquibase or a DBA pipeline can apply them from the jar the same way.
Call `apply` from an `onStart` action, so a bad `validate` setup fails the engine's start, before
anything is claimed, with a message naming the file and table it needs.

A released schema file never changes. `ExtensionSchemaReleases` keeps a snapshot of each release's
files and rows for the extension's upgrade test, like the engine's own: name the snapshot test
`<Something>SchemaSnapshot` and the release workflow takes one for each release.

## Testing an extension

- Run it in an engine on the in-memory store with a `ManualClock`, through `TaskTester`:
  `advance` runs its jobs at the new time, `drain` moves the clock past its gates' waits, and
  several engines on one store share its capability, as nodes do.
- `new BatchListener.Completed(taskId, identifier, attempt, result)` makes a result for a
  listener's unit test.
- `engine.health()` shows the topics its gates hold and how its jobs went; `engine.operatorViews()`
  its views.

## Shipped extensions

`pgtasks-rate-limit` is that pattern as a module. `RateLimit` declares `rate.per-second` and
`rate.burst`, keeps a token bucket per topic using the store's clock, and installs a claim gate:

```java
TaskEngine engine = TaskEngine.builder()
    .extension(new RateLimit())
    .configure("work", tasks -> tasks.eachTask(WORK, task -> handle(task),
        options -> options.set(RateLimit.PER_SECOND, 20.0)))
    .build();
```

With Spring, declare a `RateLimit` bean instead. A topic without a positive `rate.per-second` is
unlimited; `rate.burst` defaults to one second's worth. Both can be set in defaults and overridden
in YAML, like any other setting: `pgtasks.topics.work.rate.burst: 5`.

The limit is per node, not per cluster: each node keeps its own bucket, so three nodes can claim up
to three times the configured rate. It limits claims, not completions, and a topic still claims no
more often than its poll interval.

`ClusterRateLimit` shares a sliding-window counter across every node. Add
`pgtasks-rate-limit-postgres` alongside `pgtasks-rate-limit` when the store is Postgres; it provides
the `SlidingWindowStore` capability from a `rate_limit_window` table in the pgtasks schema, locked
a row at a time so nodes spend the same allowance in turn. That table comes from the module's own
`V001__rate_limit_window.sql`, recorded in `rate_limit_schema_history`; the engine's V001 knows
nothing about it. In `validate` mode, apply that file with your own migration tool first: without
the table, the engine doesn't start. If the counter fails once the engine runs, the topic waits and
asks again rather than claiming without a limit.

```java
TaskEngine engine = TaskEngine.builder()
    .dataSource(dataSource)
    .extension(new ClusterRateLimit())
    .configure("work", tasks -> tasks.eachTask(WORK, task -> handle(task),
        options -> options.set(ClusterRateLimit.PER_WINDOW, 60L)))
    .build();
```

`cluster.window` defaults to one minute (and must be at least a millisecond), and
`cluster.per-window`, a whole number, to zero (unlimited). Here three nodes together claim at most
60 tasks a minute. An idle topic can spend the whole minute's 60 at once; once the counter is busy,
the previous window's count fades as the next one advances, so roughly one claim is freed every
second instead of a fresh 60 appearing at the minute boundary. Both can also be set in YAML:
`pgtasks.topics.work.cluster.per-window: 60`. For a rate below one per window, use a longer window.

The engine tells each claim gate how many tasks the claim wants, so a reservation takes no more
than the batch size (or what an earlier gate allowed). It is optimistic: the gate reserves, then
returns what its claim didn't use, so another node can spend it. The engine settles every permit,
with zero when another gate held the claim back or the claim failed, and the return comes off the
window it was counted in even if the window has moved on since. A node that dies in between leaves
its reservation in the counter until its windows pass. Every node counts by the database's clock,
so nodes whose own clocks drift still agree on where the window is. A claim that uses exactly what
it reserved costs one short transaction on the topic's counter row; one that uses less costs two.

The counter is shared through the store, not through the notifier: a node that claims again after its delay reads the current state, so it needs no wake-up
message from the other nodes.
