# pgtasks

A task queue in the Postgres database you already run. You push a task in the same transaction
as the data change it reacts to, so there is no outbox table and no two-phase commit. Workers on
any number of nodes claim batches with `SKIP LOCKED`, and the engine takes care of retries, dead
letters, housekeeping, metrics and tracing.

```java
// Push, in the transaction that saves the tax return
@Transactional
public void register(TaxReturn taxReturn) {
    returns.save(taxReturn);
    tasks.push(ASSESSMENT, taxReturn.id(), new AssessmentRequest(taxReturn.id()));
}

// Handle, once the transaction has committed, on whichever node claims it
@Bean
TaskConfigurer assessmentTasks(Assessor assessor) {
    return tasks -> tasks.eachTask(ASSESSMENT, task -> assessor.assess(task.payload()));
}
```

The handler succeeds when it returns and fails when it throws. A failed task is retried with
backoff, starting at 10 s and doubling up to 30 minutes, for 7 days. After that it goes to the
dead letters, where an operator can revive or discard it. Nothing else needs configuring.

## Status

pgtasks is pre-1.0, and not on Maven Central yet: until 0.1.0 is, [install it from
source](#modules). Before 1.0, a minor version may break the API; the
[changelog](CHANGELOG.md) says what changed. The schema only ever moves forward, with
migrations the store applies itself.

## Requirements

- Java 25. The engine uses scoped values and virtual threads.
- PostgreSQL 17 to 19. CI runs the Postgres tests on each of them, and the store refuses to start on an older server. See [Postgres versions](docs/operations.md#postgres-versions).
- Spring Boot 4.1 for the starter, which is optional. The engine itself depends on no framework.

## Modules

Import the BOM, then add modules without versions:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.khopland.pgtasks</groupId>
      <artifactId>pgtasks-bom</artifactId>
      <version>0.1.0-SNAPSHOT</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

Until 0.1.0 is on Maven Central, install the snapshot from a checkout of this repository first,
with Java 25 and Maven 3.9 or later:

```sh
git clone https://github.com/khopland/pgtasks.git && cd pgtasks
mvn install -DskipTests
```

| Module | What it is |
|---|---|
| `pgtasks-spring-boot-starter` | Spring Boot: one engine from every `TaskConfigurer` bean, YAML overrides, Spring transactions, Actuator endpoint and health, Micrometer. Most applications need only this. |
| `pgtasks-engine` | The engine, without Spring: builder, configurers, retries, housekeeping, `TaskTester`. |
| `pgtasks-postgres` | The Postgres store, its schema and the 1.x importer. |
| `pgtasks-memory` | An in-memory store for local runs and fast tests. It passes the same TCK as Postgres. |
| `pgtasks-rate-limit` | Per-topic claim limits built on the extension hooks: per node, or across nodes with a shared sliding window. |
| `pgtasks-rate-limit-postgres` | The shared counter behind the cluster-wide rate limit on Postgres, with its own schema files and history table. |
| `pgtasks-micrometer` | Metrics and tracing through Micrometer Observation. |
| `pgtasks-jackson` | JSON payloads with Jackson 3, for payload types without a codec of their own. |
| `pgtasks-junit` | A JUnit 5 extension: an engine per test on the in-memory store, with a manual clock. |
| `pgtasks-api` | What application code sees: `TopicRef`, `Tasks`, handlers, `Decision`, operator views. |
| `pgtasks-spi` | What stores and integrations implement. Applications never need it directly. |

## Quick start with Spring Boot

Add `pgtasks-spring-boot-starter` and use the application's Postgres `DataSource`. The `pgtasks`
schema is created on startup; `pgtasks.schema` names another.

```java
public final class AssessmentTopics {
    public static final TopicRef<AssessmentRequest> ASSESSMENT = TopicRef.of("assessment", AssessmentRequest.class);
}

@Configuration
class AssessmentTasks {
    @Bean
    TaskConfigurer assessmentTasks(Assessor assessor) {
        return tasks -> tasks.eachTask(ASSESSMENT, task -> {
            Assessment result = assessor.assess(task.payload());
            task.push(NOTIFY, task.identifier(), new Notice(result));   // committed with this task's completion
        }, topic -> topic.concurrency(4).ordering(Ordering.SINGULAR_BY_IDENTIFIER));
    }
}
```

Records and other payload types without a codec are stored as JSON when Jackson is on the
classpath. Binary formats such as Protobuf take `PayloadCodec.binary(Invoice::toByteArray, Invoice::parseFrom)`,
stored as Base64 text. To push from anywhere, inject `Tasks`:

- `tasks.push(...)` joins the current transaction and throws if there is none.
- `tasks.on(connection).push(...)` joins the transaction on an explicit JDBC connection.
- `tasks.independently().push(...)` commits on its own.

For a web dashboard over the Actuator endpoint, set `pgtasks.dashboard.enabled=true` and open
`/pgtasks/` ([Dashboard](docs/operations.md#dashboard)).

Only what differs per environment goes in YAML:

```yaml
pgtasks:
  topics:
    assessment: { concurrency: 16, retry.give-up-after: 2d }
```

A topic or setting that no code defines fails startup, so a typo can't go unnoticed.

## Quick start without Spring

```java
JdbcTransactions transactions = new JdbcTransactions(dataSource);
TaskEngine engine = TaskEngine.builder()
    .dataSource(dataSource)                    // Postgres; or .store(new InMemoryTaskStore()) for local runs
    .transactions(transactions)                // what tasks.push(...) joins
    .codecs(new JacksonPayloadCodecs(JsonMapper.builder().build()))
    .configure(new AssessmentTasks(assessor))  // a TaskConfigurer
    .build()
    .start();

transactions.inTransaction(connection -> {
    returns.save(connection, taxReturn);
    engine.tasks().push(ASSESSMENT, taxReturn.id(), new AssessmentRequest(taxReturn.id()));  // same commit
    return null;
});

engine.close();                                // drains running batches, then stops
```

## Further reading

- [Quickstart](examples/quickstart/README.md): a runnable Spring Boot application and a five-minute tour.
- [When to use pgtasks, and when not](docs/choosing.md): what it is good at, where another tool fits better, and how it compares with db-scheduler, JobRunr, Quartz and brokers.
- [Concepts](docs/concepts.md): topics, pushing, handlers and decisions, ordering, insertion and retries.
- [Patterns](docs/patterns.md): effects that must happen exactly once, fan-out and fan-in, producers and consumers in separate services.
- [Testing](docs/testing.md): the JUnit extension, `TaskTester`, and Spring tests.
- [Writing an extension](docs/extensions.md): adding a feature through the engine's hooks: jobs, claim gates, batch listeners, settings and store capabilities.
- [Operations](docs/operations.md): schema, endpoint, dashboard, health, metrics and alerts, dead letters, retention, pool sizing and deploys.
- [Migrating from 1.x](docs/migrating-from-1.x.md): mapping the concepts, and moving queued tasks with `LegacyImport`.
- [Changelog](CHANGELOG.md) and [how releases are made](RELEASING.md).
- Benchmarks: [the schema](docs/benchmarks/2026-11-20-schema.md), and [wake-ups, throughput ceiling and latency](docs/benchmarks/2026-11-22-notify-and-throughput.md).

## Contributing

Bug reports and questions go in the [issues](https://github.com/khopland/pgtasks/issues), and
pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) says how to build and test, and
what a change needs. Report a security problem privately instead, as
[SECURITY.md](SECURITY.md) describes.

## License

[Apache License 2.0](LICENSE). pgtasks started as a rewrite of
[Skatteetaten's task library](https://github.com/Skatteetaten/skatteprosessen-task) (1.x) and
keeps its license. 1.x stays in that repository.
