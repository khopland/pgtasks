# Writing an extension

The engine's core is small: topics, pushing in the caller's transaction, claiming, retries, dead
letters and housekeeping. Features beyond that, such as a rate limit, a circuit breaker or
recurring tasks, belong in extensions. One ships: [`pgtasks-rate-limit`](#shipped-extensions). An
extension uses the same hooks the engine's own housekeeping uses, so it needs no change to the
engine. Everything an extension implements or receives is in the package
`io.github.khopland.pgtasks.engine.extension`.

```java
public final class NightlyReport implements Extension {

    @Override
    public void install(ExtensionContext context) {
        context.job(HousekeepingJob.leader("nightly report", Duration.ofMinutes(1), () -> {
            // runs on the node that holds the housekeeping lease, once across the cluster
        }));
    }
}
```

Register it with `TaskEngine.builder().extension(new NightlyReport())`. With Spring, make it a
bean: the starter installs every `Extension` bean, in `@Order`. An extension is installed once,
when the engine is built; `TaskEngine.extensions()` lists the installed ones.

## The hooks

| Hook | What it does |
|---|---|
| `job(HousekeepingJob)` | Runs work on a fixed delay, on every node (`everyNode`) or only on the housekeeping leader (`leader`). A job that throws is logged and runs again; each extension job has a thread of its own. Leader jobs also run under `TaskTester.advance`. |
| `claimGate(ClaimGate)` | Asked before every claim on this node: claim at most `n` tasks (`Permit.atMost`), or none for a while (`Permit.waitFor`). The strictest gate wins. `claimed(topic, n)` tells it what the claim got. Return `Permit.ANY` when nothing limits the claim. |
| `batchListener(BatchListener)` | Told what became of each task of a batch once its completion committed: succeeded, filtered, suspended, delayed, retrying, dead-lettered. |
| `handlerDecorator(HandlerDecorator)` | Wraps the handler of every topic this node runs; the first registered is outermost. |
| `observer(TaskObserver)` | Sees this node's pushes and batches, for tracing, metrics, or headers that follow the work, next to the observers given to the builder. For a header name both give, the builder's value is stored. One that throws is logged and ignored. |
| `channel(name, reload)` | Reloads something that may have changed on another node. The reload runs on a message on the channel, after a lost notification connection and on every heartbeat. Send a message with `store().notify(transaction, name, payload)` in the transaction that made the change. |
| `onStart` / `onStop` | Runs each time the engine starts (after its topics run) and stops (before they stop), restarts included. |

And what it can use:

- `store()`, `tasks()`, `clock()`: the engine's store, its producer API, and the store's clock.
- `leading()`: whether this node holds the housekeeping lease now. A leader job that loops over
  batches stops when it turns false.
- `topicSetting(topic, key)`: a topic's value of one of the extension's settings.
- `capability(Type.class)`: the extension's own view of the store (below).

## Settings

An extension declares per-topic settings with `SettingKey`, a dotted lowercase name, a type, a
default and a parser for overrides:

```java
static final SettingKey<Double> RATE = SettingKey.of("rate.per-second", Double.class, 0.0, Double::valueOf);

@Override
public List<SettingKey<?>> settings() {
    return List.of(RATE);
}
```

Topics set it like any option, `options.set(RATE, 20.0)`, in defaults too. Overrides and Spring
YAML use the name: `pgtasks.topics.assessment.rate.per-second: 50`. The value layers like the
engine's own settings, and `engine.effectiveSettings()` and the endpoint show it with its source.

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

An extension that needs a table ships its SQL in its own jar; it does not add files to the engine's
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
applications using Flyway, Liquibase or a DBA pipeline can apply them from the jar the same way. An
extension calls `apply` before it first uses its tables, best from an `onStart` action, so a bad
`validate` setup fails the engine's start with a message naming the file and table it needs. A
claim gate that throws is skipped and the claim goes ahead, so a gate that must hold claims back
when its storage fails catches the error and returns a wait itself.

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
