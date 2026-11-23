# Testing

Handlers are plain code, so most tests need no engine at all. When the queue's behaviour
matters (retries, ordering, what gets pushed downstream), run a real engine on the in-memory
store. It passes the same TCK as Postgres, so ordering, insertion policies, fencing, junctions,
and retries behave the same. Keep a few tests on Postgres for what only Postgres can
prove: that a push commits and rolls back with your own tables.

## JUnit 5, without Spring

`pgtasks-junit` gives each test a fresh engine on the in-memory store with a manual clock:

```java
class NotifyTasksTest {

    @RegisterExtension
    static TaskEngineExtension engine = TaskEngineExtension.of(new NotifyTasks(new FakeMailer()));

    @Test
    void sends_and_archives(Tasks tasks, TaskTester tester, LocalTransactions transactions) throws Exception {
        transactions.inTransaction(() -> tasks.push(NOTIFY, "anna", "hello"));

        tester.drain(NOTIFY);                       // runs it, jumping the clock past retries and delays

        tester.assertPushed(ARCHIVE, 1);
        assertThat(tester.pushedPayloads(ARCHIVE)).containsExactly("sent hello");
    }
}
```

Test methods can take `TaskEngine`, `Tasks`, `TaskTester`, `TaskAdmin`, `ManualClock` and
`LocalTransactions`. `TaskEngineExtension.with(builder -> ...)` sets up anything else, such as
globals or codecs.

## TaskTester

- `awaitIdle()`: waits until nothing is left to do now, without moving the clock.
- `drain(topic)`: runs a topic until nothing in it can run, jumping the clock past delays,
  and backoffs. Suspended tasks and dead letters stay.
- `advance(duration)`: moves the clock and runs housekeeping once, as if that much time passed.
- `pushed(topic)`, `pushedPayloads(topic)`, `pushedIdentifiers(topic)`, `assertPushed(topic, n)`,
  `assertNothingPushed(topic)`: what was pushed and committed.
- `counts(topic)` and `task(id)`: the queue and one task, as an operator sees them.

## Spring

`pgtasks.store=memory` runs the application without a database. `pgtasks.testing=true` adds a
`TaskTester` bean:

```java
@SpringBootTest(properties = {"pgtasks.store=memory", "pgtasks.testing=true"})
class ShippingTest {
    @Autowired Tasks tasks;
    @Autowired TaskTester tester;

    @Test
    void ships_and_invoices() {
        tasks.independently().push(SHIPMENTS, "o-1", new Shipment("o-1"));
        tester.awaitIdle();
        tester.assertPushed(BillingTopics.INVOICES, 1);
    }
}
```
