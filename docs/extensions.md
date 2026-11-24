# Writing an extension

The engine's core is small: topics, pushing in the caller's transaction, claiming, retries, dead
letters and housekeeping. Features beyond that, such as a rate limit, a circuit breaker or
recurring tasks, belong in extensions; none ships with pgtasks yet. An extension uses the same
hooks the engine's own housekeeping uses, so it needs no change to the engine. Everything an
extension implements or receives is in the package `io.github.khopland.pgtasks.engine.extension`.

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
engine's store, that fails with a message naming the missing module. An extension's own tables are
its own to create, for now: the schema runner applies only the engine's files.

## An example

`NodeRateLimit`, in the engine's tests, is a per-node rate limit in one file: a `SettingKey` for
the rate, a `ClaimGate` that keeps a token bucket per topic, and nothing else.
