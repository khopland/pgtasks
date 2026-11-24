# Patterns

## What runs once, and what may run twice

Pushing is transactional: a task exists if and only if the transaction that pushed it
committed. Processing is at least once. A handler runs, then the engine records the task's
decision in a transaction of its own. If the node dies in between, or the completion fails, the
task runs again. Completions are fenced, so a stale worker can never finish a task that another
node has claimed since. It can still have done the work, though.

So decide, for every effect a handler has, which of these it is:

- **Downstream tasks.** Push them with `task.push(...)`. They are written in the completion's
  transaction, so they exist exactly when the task succeeded.
- **Writes to the same database.** Put them in a `BatchResource`'s `beforeCommit`, which runs
  on the completion's connection, so they commit or roll back with it:

  ```java
  final class Ledger implements BatchResource<List<Entry>> {
      public List<Entry> open(TopicRef<?> topic, List<Task<?>> tasks) {
          return new CopyOnWriteArrayList<>();            // the handler stages entries here
      }

      public void beforeCommit(List<Entry> staged, Decisions decisions, Connection connection) throws SQLException {
          // insert the entries whose task's final decision continues, on this connection
      }
  }

  tasks.topic(ASSESSMENT, batch -> {
      List<Entry> ledger = batch.resource(LEDGER);
      return batch.decide(task -> { ledger.add(entryFor(task)); return Decision.success(); });
  }, options -> options.resources(LEDGER));
  ```

  Every resource first reviews the decisions and may make them stricter (`review`). Only then
  does anything get written, and every resource sees the same final decisions, so nothing is
  written for a task another resource stopped.
- **Calls to other systems.** These can't be rolled back. Make them idempotent with
  `task.idempotencyKey()` (topic and task id), which stays the same across automatic retries.
  `afterCommit` is for wake-ups and other effects after the commit. It is not guaranteed to run:
  if the commit succeeded but its reply was lost, the engine can't tell, so it doesn't run it.

## Fan-out and fan-in

Fan-out is pushing several downstream tasks. For fan-in, push the task that waits for the parts
as an ordinary task, and let its handler check whether every part is done, in the application's
own tables, deciding `later(delay)` until they are:

```java
// The aggregate, pushed with the parts.
task.push(MERGE, invoiceId, request);

// Its handler: done once every line is.
if (!lines.allDone(invoiceId)) {
    return Decision.later(Duration.ofSeconds(5));
}
```

`later()` doesn't use up an attempt. The order-service example ships an order this way once it is
both paid and reserved.

## Producers and consumers in separate services

A service that only pushes declares the topics it pushes to, so they exist whichever service is
deployed first, and so its codecs are checked at startup:

```java
TaskConfigurer orderEvents = tasks -> tasks.pushesTo(BillingTopics.INVOICES, ShippingTopics.SHIPMENTS);
```

With Spring, `pgtasks.enabled=false` makes a node only push. It applies the schema and registers its
topics, but claims nothing, runs no housekeeping and doesn't listen for notifications.
