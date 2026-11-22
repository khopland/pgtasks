# NOTIFY at commit, throughput ceiling and wake-up latency

Run on 2026-11-22 with `NotifyBenchmark` in `pgtasks-benchmark` at commit 9c40dc1 (b2b4d93 once rebased onto main), before the
schema was renamed from `tasks` to `pgtasks`. The benchmark now runs on the renamed schema; only a
3-second smoke run has checked that, and its numbers aren't comparable with these. It ran on the real
engine and schema, with **`fsync`, `synchronous_commit` and `full_page_writes` on**. The question:
pgtasks wakes workers with `pg_notify('pgtasks_ready', topic)` from a statement-level trigger on
`pgtasks.queue`, so every push notifies inside the application's own transaction. Postgres
serializes the commits of all transactions that notified behind one database-wide lock
(`PreCommit_Notify` takes a lock on object 0 of `pg_database` and holds it until the commit has
been flushed). Does that cap how fast an application with many concurrent transactions can commit?

## Short answer

- **Yes, and at realistic concurrency.** Once 8 or more transactions push at the same time, the
  shipped trigger limits the whole database to about one pushing commit per WAL flush, whatever
  the number of producers. Pushing commits can no longer share a flush (group commit).
  - On devbox (real flush, ~2 ms): 490 commits/s at 32 producers against 3,114 to 4,028 without
    the notify. At 64 it's 491 against 4,505.
  - On the Mac with a real flush (`fsync_writethrough`, ~7 ms): 120 against 3,816 at 32, and 136
    against 5,164 at 64.
  - On the Mac's default settings, where macOS doesn't flush the drive's cache: about 4,000 to
    7,900 against 10,000 to 16,600.
  - Commit latency grows with the queue for the lock. On devbox commit p99 was 178 ms at 64
    producers and 1.2 s at 128; on the Mac with a real flush it was 1.3 s at 64.
- **The cause is confirmed, not inferred.** With the trigger, an average of 6 of 8, 30 of 32, 62 of
  64 and 125 of 128 producer backends were waiting on `Lock:object` for object 0 of `pg_database`.
  The trigger had 0.5 to 0.9 commits per WAL flush; without the notify, 1.6 to 23 at 8 or more
  producers.
- **One producer barely notices.** A single serial producer pays one flush per commit anyway: 271
  against 389 commits/s on devbox, 118 against 146 on the Mac with a real flush.
- **Recommended fix: a debounced notify sent by the engine after commit, on its own connection**
  (details under [Recommendation](#recommendation)). In every setup it committed as fast as having
  no notify at all, within the noise, with about 80 to 100 notifications a second instead of one
  per push. It costs 1 to 3 ms of p50 wake-up latency (p99 up to about 12 ms on the Mac).
- **Notifying only into an idle topic doesn't work.** It skipped the notify when the topic already
  had a ready task, and lost wake-ups at once: p50 latency of 15 s at 1,000 tasks/s on the Mac, and
  already at 100 tasks/s on devbox. Its extra index probe also made pushes slower on the Mac.
- **Throughput ceiling, as shipped:** with one task per transaction, the notify lock is the limit
  (devbox 330 to 480 tasks/s, Mac 3,800 to 7,500). With 100 tasks per transaction it isn't: one
  node sustained 7,600 to 9,000 tasks/s on devbox and 20,000 to 21,000 on the Mac, three nodes
  13,900 to 15,200 and 33,000 to 47,000.

## Machines and settings

| | Mac | devbox |
|---|---|---|
| Machine | Apple M4 (4 performance and 6 efficiency cores), 16 GB, internal SSD | Proxmox LXC, Debian 13, 4 cores of an i5-8500T shared with other guests, 6 GB, consumer NVMe |
| OS | macOS 27.0.1 | Debian 13 |
| JDK | Temurin 25.0.3 | OpenJDK 25 (SDKMAN) |
| `wal_sync_method` | `open_datasync` (default), and one run with `fsync_writethrough` | `fdatasync` (default) |

Both: Postgres 17.12, embedded (zonky binaries), on the same machine as the client. `fsync`,
`synchronous_commit` and `full_page_writes` on; `shared_buffers` 1GB, `max_wal_size` 8GB,
`checkpoint_timeout` 15min, `commit_delay` 0, `max_connections` 400, `wal_level` replica. pgjdbc
42.7.14, HikariCP.

macOS `fsync` doesn't flush the drive's write cache, so the Mac's default numbers are what a
machine with a battery-backed or no-flush cache would see. `fsync_writethrough` makes Postgres
use `F_FULLFSYNC`, a real flush. Compare devbox numbers only with devbox numbers.

## Workload

Each run gets a fresh database copied from a template: the schema, an application table
`app.event`, and 200,000 finished tasks in the topic, analyzed. Without those statistics the
planner completes a batch through the wrong index (see [Found on the way](#found-on-the-way)), and
the consumer, not the producers, would set the pace. A `CHECKPOINT` runs before each run.

**Wake-up variants:**

| Variant | What it does |
|---|---|
| trigger as it is | `R__notify_ready.sql` as shipped: every push notifies inside the application's transaction |
| no notify (baseline) | the insert trigger dropped; nothing notifies |
| notify only into an idle topic | the insert trigger notifies only if the topic had no other ready task |
| debounced notify after commit | the insert trigger dropped; after each commit the producer tells a debouncer, which sends `SELECT pg_notify(...)` in autocommit on its own connection, at once after a quiet spell and then at most once per 10 ms. This prototypes in the benchmark what the engine would do. |

**The three parts:**

1. **Producer contention:** N producers (1, 8, 32, 64, 128), each on its own connection, committing
   transactions that insert one row into `app.event` and push one task with `tasks.on(connection)`.
   One engine node consumes (LISTEN on, default batches, 1 s poll so the baseline still drains).
   3 s warm-up, then 15 or 20 s measured. Wait events were sampled every 5 ms from
   `pg_stat_activity`, joined with `pg_locks` for ungranted locks. WAL flushes come from
   `pg_stat_wal` (`wal_sync`, or `wal_write` with an `open_*sync` method).
2. **Throughput ceiling:** producers push flat out while 1 or 3 engine nodes claim and complete
   (no-op handler, batches of 100, 2 or 8 batches per node). The nodes are **separate engines in one
   JVM**, each with its own pool, sharing the machine with Postgres. Then a fresh run pushes at 90 %
   of the lower of what was pushed and completed; it held if the backlog grew by less than a
   second's worth. Two shapes: 64 producers with one task per transaction, and 8 producers with 100.
3. **End-to-end latency:** 8 producers at a steady 100 or 1,000 tasks/s in total, one task per
   transaction, one engine node with default settings (2 batches of 100, 30 s poll). Latency is
   from the producer's `commit()` returning to the handler starting.

## Results

### Producer contention

Commits/s, with commit p99 in ms in brackets.

**devbox**, one round of 15 s:

| Producers | trigger as it is | no notify | notify only into an idle topic | debounced after commit |
|---:|---:|---:|---:|---:|
| 1 | 271 (20.5) | 389 (18.7) | 458 (3.6) | 284 (21.0) |
| 8 | 513 (27.3) | 2,353 (14.0) | 2,205 (7.3) | 2,372 (7.7) |
| 32 | 490 (93.0) | 3,114 (35.1) | 3,865 (11.0) | 4,028 (26.5) |
| 64 | 491 (178.0) | 4,505 (16.4) | 3,857 (20.7) | 4,388 (16.1) |
| 128 | 375 (1,238) | 4,172 (43.6) | 3,610 (35.5) | 4,253 (29.4) |

**Mac, real flush** (`wal_sync_method=fsync_writethrough`), one round of 15 s:

| Producers | trigger as it is | no notify | debounced after commit |
|---:|---:|---:|---:|
| 1 | 118 (15.8) | 146 (12.9) | 133 (16.8) |
| 8 | 122 (79.0) | 1,091 (11.3) | 988 (17.8) |
| 32 | 120 (502) | 3,816 (18.1) | 3,238 (17.0) |
| 64 | 136 (1,288) | 5,164 (20.5) | 5,409 (16.3) |

**Mac, default** (`open_datasync`, no drive-cache flush), median of three rounds of 20 s:

| Producers | trigger as it is | no notify | notify only into an idle topic | debounced after commit |
|---:|---:|---:|---:|---:|
| 1 | 6,194 (0.3) | 5,518 (1.3) | 3,652 (1.7) | 4,934 (2.0) |
| 8 | 9,505 (2.8) | 11,625 (2.5) | 6,867 (6.2) | 8,935 (3.8) |
| 32 | 5,533 (28.0) | 10,002 (7.6) | 11,635 (9.7) | 19,514 (5.5) |
| 64 | 7,856 (19.1) | 16,646 (16.3) | 12,466 (19.1) | 16,699 (13.6) |
| 128 | 3,965 (159) | 14,620 (31.0) | 13,257 (39.5) | 14,818 (30.1) |

The rounds spread widely here (at 32 producers the debounced variant gave 13,854, 19,514 and 24,722;
the trigger gave 4,415, 5,533 and 8,515), but the trigger was the slowest in every round from 32
producers up. At 1 and 8 producers the variants overlap. A flush that costs almost nothing leaves
little to serialize.

**Where the producers waited.** Average number of producer backends in each state, the top two:

| Setup | Producers | trigger as it is | no notify |
|---|---:|---|---|
| devbox | 8 | 6.2 `Lock:object (pg_database 0)`; 0.5 `IO:WalSync` | 3.9 `LWLock:WALWrite`; 0.8 `IO:WalSync` |
| devbox | 64 | 62.2 `Lock:object (pg_database 0)`; 0.5 `IO:WalSync` | 21.5 `LWLock:BufferContent`; 16.0 `LWLock:WALWrite` |
| devbox | 128 | 124.8 `Lock:object (pg_database 0)`; 0.5 `LWLock:WALWrite` | 66.8 `LWLock:BufferContent`; 26.0 `LWLock:WALWrite` |
| Mac, real flush | 64 | 62.8 `Lock:object (pg_database 0)`; 0.5 `LWLock:WALWrite` | 39.3 `LWLock:WALWrite`; 9.4 `LWLock:BufferContent` |
| Mac, default | 128 | 114.8 `Lock:object (pg_database 0)`; 5.7 `LWLock:BufferContent` | 44.4 `LWLock:BufferContent`; 38.5 `LWLock:WALWrite` |

Without the notify, producers wait in `LWLock:WALWrite`, which is group commit working: one
backend flushes for everyone queued behind it. Commits per flush on devbox at 64 producers: 0.7
with the trigger, 10.6 without and 10.2 debounced. On the Mac with a real flush: 0.5, 22.4 and 23.2.

**Producers outpacing one consumer.** Without the trigger, 64 or more producers flat out committed
faster than one engine node with 2 batches could complete, so the backlog grew. On devbox it
reached 14,000 to 22,000 in 15 s; on the Mac, 130,000 to 250,000. The trigger kept the backlog at
zero only by holding the producers back. On the Mac the producers' 64 to 128 busy backends also
took CPU from the consumer (it completed 1,900 to 5,900 tasks/s, against 23,000 to 30,000 when
the producers push batches). The claim and completion plans were the right ones (checked with `auto_explain`).

### Throughput ceiling

Sustained tasks/s: the paced rate held, and what completed at it.

| | Nodes | Batches per node | devbox, trigger | devbox, debounced | Mac, trigger | Mac, debounced |
|---|---:|---:|---:|---:|---:|---:|
| 64 producers × 1 task | 1 | 2 | 434 | 2,844 | 4,751 | 8,428 |
| 64 producers × 1 task | 1 | 8 | 484 | 3,263 | 4,174 | 7,295 |
| 64 producers × 1 task | 3 | 2 | 393 | 3,587 | 3,757 | 7,341 |
| 64 producers × 1 task | 3 | 8 | 330 | 2,996 | 7,512 | 11,045 |
| 8 producers × 100 tasks | 1 | 2 | 7,640 | 6,347 | 21,155 | 14,512 |
| 8 producers × 100 tasks | 1 | 8 | 9,020 | 5,847 | 20,428 | 26,865 |
| 8 producers × 100 tasks | 3 | 2 | 13,880 | 10,433 | 33,456 | 40,247 |
| 8 producers × 100 tasks | 3 | 8 | 15,200 | 12,300 | 46,939 | 33,691 |

Flat out, the batched producers pushed 24,000 to 47,000 tasks/s on devbox and 86,000 to 138,000
on the Mac, so the consumers were the limit there.

- **One task per transaction:** with the trigger, the notify lock is the ceiling, and more nodes
  don't help (devbox 330 to 484 tasks/s with 1 or 3 nodes). With the debounced notify it was 6.5 to
  9 times higher on devbox and 1.5 to 2 times on the Mac. The consumers then set the limit.
- **100 tasks per transaction:** a push transaction is one commit for 100 tasks, so the notify lock
  hardly matters. The debounced column differs from the trigger's in both directions, which is
  the method's noise: the paced rate is 90 % of a flat-out run, and the flat-out runs load the
  shared CPU differently from one run to the next. Read these rows as "the trigger doesn't limit
  batched pushes", not as one variant beating the other.
- **Three nodes** gave 1.6 to 2.3 times one node's rate with batched pushes and the trigger. Everything ran on one
  machine (4 cores on devbox), so a real deployment, with nodes on their own machines, should
  scale further.

### End-to-end latency

From push commit to handler start, ms. One run each; LISTEN off ran 120 s, the others 30 s.

| Wake-up | Rate/s | Mac p50 | Mac p99 | Mac max | devbox p50 | devbox p99 | devbox max |
|---|---:|---:|---:|---:|---:|---:|---:|
| trigger as it is, LISTEN on | 100 | 2.1 | 4.1 | 24 | 4.9 | 8.8 | 52 |
| debounced after commit, LISTEN on | 100 | 3.3 | 11.7 | 15 | 5.2 | 11.7 | 15 |
| notify only into an idle topic, LISTEN on | 100 | 1.8 | 3.5 | 9 | 15,087 | 29,707 | 30,016 |
| LISTEN off (poll every 30 s) | 100 | 15,019 | 29,689 | 30,011 | 15,096 | 29,688 | 30,013 |
| trigger as it is, LISTEN on | 1,000 | 1.6 | 6.3 | 50 | 4.5 * | 22.1 * | 328 * |
| debounced after commit, LISTEN on | 1,000 | 5.3 | 11.6 | 32 | 4.6 | 69.6 | 995 |
| notify only into an idle topic, LISTEN on | 1,000 | 15,307 | 29,721 | 30,011 | 15,966 | 29,686 | 30,024 |
| LISTEN off (poll every 30 s) | 1,000 | 15,326 | 29,694 | 30,010 | 5.8 * | 29,196 * | 29,999 * |

\* On devbox, with the trigger, 8 producers can't commit 1,000 pushes a second: those runs
reached about 460 (trigger) and 550 tasks/s (LISTEN off, where the trigger still runs). With
LISTEN off at that rate the queue never drained, so the worker kept claiming: p50 5.8 ms, but 24,000
of 66,000 tasks waited over a second.

- **LISTEN off** means waiting for the poll: p50 about half the 30 s interval.
- **Debounced** adds 1 to 3 ms at p50 and stays under the 10 ms window plus a claim at p99 on the
  Mac. The devbox p99 of 70 ms at 1,000/s comes with a busy 4-core machine, where the plain trigger
  had a 328 ms max at half the rate.
- **Notify only into an idle topic** loses wake-ups by design. A push that sees a ready task skips
  the notify, but that task may be one a worker is claiming right then. The worker's next claim
  can run on a snapshot from before the push committed, find nothing, and wait for the poll. At
  100 tasks/s on the Mac the queue was empty at almost every push, so it notified. At 1,000/s, and
  already at 100/s on devbox, most tasks waited for the 30 s poll.

## Recommendation

**The contention is real, from about 8 concurrent pushing transactions** on any machine whose
flush costs real time (most production servers, any cloud disk). An application that pushes one
task per request, with dozens of requests in flight, gets its database's commit rate capped near
one commit per flush, about 500/s on devbox, and its commit latency goes up with the number of
waiters. It affects every transaction that pushes, not just pgtasks', and only them:
transactions that don't notify aren't serialized. A low-concurrency or batch-pushing application
won't notice.

**Fix: notify after commit, debounced, from the engine.** Drop the INSERT trigger. When a push's
transaction commits, the engine wakes its own workers for the topic (it already does for
`independently()`). It also asks a per-node notifier to send `pg_notify('pgtasks_ready', topic)` in a
short transaction of its own: at once after a quiet spell, then at most once per topic per 10 ms.
Only those short transactions queue for the notify lock, at most about 100 a second per node and
topic, and the application's commits share flushes again. The benchmark's prototype matched the
no-notify baseline at every producer count, in all three setups.

Trade-offs:

- **Wake-up latency:** +1 to 3 ms at p50, at most the debounce window (10 ms) more at the tail. The
  window could be shorter; 10 ms already cut notifications to about 80 to 100 a second.
- **Missed notifications:** a node that dies between the commit and the notify loses that wake-up.
  Other nodes find the task at their next poll (default 30 s), as they do today when a
  notification is dropped. The notify is a hint now too; polling still finds the work.
- **Pushes the engine doesn't see commit:** `tasks.push(...)` inside a bound transaction (Spring,
  `JdbcTransactions`) and staged pushes from handlers have a commit hook. `tasks.on(connection)`
  doesn't, and neither do rows inserted by SQL outside the library. Those would rely on the poll,
  or the application calls a `tasks.wake(topic)` after its commit. If that matters, keep the
  trigger as an option (say `pgtasks.notify=trigger|after-commit|off`) for applications with few
  concurrent producers.
- **The UPDATE trigger** (housekeeping promotions, released tasks) runs in the engine's own
  transactions, at a low rate. It can stay, or use the same notifier.

**Alternatives measured or considered:**
- Notify only into an idle topic: rejected (lost wake-ups, and its probe slowed pushes on the Mac).
- LISTEN off, polling only: costs half the poll interval in latency, or a short poll interval's
  empty claims on every node.
- Leave it as is and document it: an option for low concurrency, but the cap is easy to hit and
  hard to diagnose (it shows up as slow commits across the whole database, in `Lock:object`).

Nothing in the library has changed yet; this is a proposal.

## Found on the way

- **Completion walks the topic's history while `pgtasks.task` has no statistics.** On a fresh
  database, before the first autoanalyze, the completion's UPDATE of `pgtasks.task` used
  `task_completed_idx` with only `topic_id` as its condition. It read every task of the topic for
  each batch: 3,677 rows for 100 updated in one plan, and more with every task. The consumer fell to
  about 50 to 100 tasks/s with a growing backlog. Once `pgtasks.task` is analyzed the plan uses
  `task_pkey`. This is the open issue about a completion plan linear in the topic's own rows. It
  matters for a new installation hit by a burst before autovacuum's first analyze; pinning the
  plan, like the claim's, would close it. Not fixed here.

## Caveats

- **Embedded Postgres on the client's machine.** Producers, engines and Postgres share the CPU
  (10 cores on the Mac, 4 on devbox). From 64 producers up the CPU is contended, and the
  no-notify runs show `LWLock:BufferContent` on the queue's hot index pages.
- **Short runs.** 15 to 20 s per point, one round on devbox and with `fsync_writethrough`, three on
  the Mac's default settings. Mac rounds varied by up to 2x at the same point; devbox looked
  steadier but wasn't repeated. The conclusions rest on gaps of 4x to 40x, well outside that noise.
- **The debounced notify is a prototype in the benchmark**, driven by the producers after commit,
  not engine code. An engine version would also wake the local node at once.
- **Engines in one JVM.** The three "nodes" are separate engines with separate pools, LISTEN
  connections and node rows, in one process.
- **A no-op handler** and a 7-byte payload: the ceiling is the library's and the database's, not an
  application's.
- **Postgres 17.** The notify lock is the same in every supported version.

To rerun:

```
mvn -Pbenchmark -pl pgtasks-benchmark -am test -Dtest=NotifyBenchmark -Dsurefire.failIfNoSpecifiedTests=false -Dbenchmark.notify=all
```

Options are listed in the class's Javadoc (producer counts, variants, durations, repeats, extra
server settings such as `-Dbenchmark.postgres.config=wal_sync_method=fsync_writethrough`).
