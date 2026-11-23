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

Fan-out is pushing several downstream tasks. Fan-in waits for a group of them: push the waiting
task suspended, and let each part stage a junction for the group.

```java
// The aggregate waits until every part has arrived.
task.push(MERGE, invoiceId, Insertion.APPEND, Creation.of(invoiceId, request).asSuspended());

// Each part, when it succeeds, checks the group in the target topic.
part.junction(MERGE, Junction.Counts.SUSPENDED, Set.of("line-1", "line-2", "line-3"));
```

Once every identifier in the group (the staging task's own plus the given ones) has a task
there that counts, the group's suspended tasks become ready. `Counts.SUSPENDED` counts only
suspended tasks: every part is waiting. `Counts.ANY` counts any task that wasn't made redundant,
finished or not.

`Counts.ANY` looks at the tasks the topic still keeps. Retention deletes finished tasks per
outcome (succeeded after 30 days by default), and a deleted part no longer counts. If one part
succeeded 31 days before the last one arrives, the group never completes. Set the target topic's
retention for `SUCCEEDED` (and any other outcome that should count) longer than a group can take,
for example `options.retain(Outcome.SUCCEEDED, Period.ofDays(90))`.

## Producers and consumers in separate services

A service that only pushes declares the topics it pushes to, so they exist whichever service is
deployed first, and so its codecs are checked at startup:

```java
TaskConfigurer orderEvents = tasks -> tasks.pushesTo(BillingTopics.INVOICES, ShippingTopics.SHIPMENTS);
```

With Spring, `pgtasks.enabled=false` makes a node only push. It applies the schema and registers its
topics, but claims nothing, runs no housekeeping and doesn't listen for notifications.
