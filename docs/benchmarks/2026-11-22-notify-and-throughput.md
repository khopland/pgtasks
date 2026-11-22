# NOTIFY at commit, throughput ceiling and wake-up latency

Run on 2026-11-22 with `NotifyBenchmark` in `pgtasks-benchmark` (commit 9ce3778), on the real engine
and schema (`pgtasks`), with **`fsync`, `synchronous_commit` and `full_page_writes` on**. The
question: pgtasks wakes workers with `pg_notify('pgtasks_ready', topic)` from a statement-level
trigger on `pgtasks.queue`, so every push notifies inside the application's own transaction.
Postgres serializes the commits of all transactions that notified behind one database-wide lock
(`PreCommit_Notify` takes a lock on object 0 of `pg_database` and holds it until the commit has
been flushed). Does that cap how fast an application with many concurrent transactions can commit?

## Short answer

- **Yes, and at realistic concurrency.** From about 8 concurrent pushing transactions, the shipped
  trigger limits the whole database to about one pushing commit per WAL flush, whatever the number
  of producers. Pushing commits can no longer share a flush (group commit). Medians of three rounds:
  - devbox (real flush): 441 to 522 commits/s from 8 to 128 producers, against 2,479 to 4,273
    without the notify.
  - Mac with a real flush (`fsync_writethrough`): 102 to 113 commits/s from 8 to 128 producers,
    against 820 to 6,009.
  - Mac on its default settings, where macOS doesn't flush the drive's cache: 3,856 to 8,254
    against 10,333 to 22,691.
  - Commit latency grows with the queue for the lock: commit p99 at 128 producers was 1.7 s on
    devbox and 2.1 s on the Mac with a real flush, against 26 and 35 ms without the notify.
- **The cause is confirmed, not inferred.** With the trigger, an average of 6 of 8, 30 of 32, 62 of
  64 and 125 of 128 producer backends were waiting on `Lock:object` for object 0 of `pg_database`.
  Without a consumer and from 8 producers up, the trigger ran at 1.0 commits per WAL flush on every
  machine; without the notify, 1.6 to 29.
- **One producer barely notices.** A single serial producer pays one flush per commit anyway: 369
  against 485 commits/s on devbox, 112 against 111 on the Mac with a real flush.
- **Recommended fix: a debounced notify sent by the engine after commit, on its own connection**
  (details under [Recommendation](#recommendation)). With a real flush on the Mac it committed
  within 1 to 4 % of having no notify at all. On devbox, with a consumer, it reached 80 to 100 %
  from 8 producers up (the rounds are noisy), and without a consumer it matched. It sent 80 to 100
  notifications a second instead of one per push, and cost 1.6 to 3.5 ms of p50 wake-up latency on
  the Mac and nothing measurable on devbox.
- **Notifying only into an idle topic doesn't work.** It skipped the notify when the topic already
  had a ready task, and lost wake-ups: p50 latency of 15 s at 1,000 tasks/s on the Mac, and already
  at 100 tasks/s on devbox.
- **Throughput ceiling, as shipped:** with one task per transaction, the notify lock is the limit,
  and more nodes don't help: devbox 328 to 413 tasks/s, Mac 2,737 to 4,044. With 100 tasks per
  transaction it isn't: one node sustained 7,660 to 8,907 tasks/s on devbox and 15,466 to 17,955
  on the Mac, three nodes 11,647 to 14,213 and 36,891 to 43,359.

## Machines and settings

| | Mac | devbox |
|---|---|---|
| Machine | Apple M4 (4 performance and 6 efficiency cores), 16 GB, internal SSD | Proxmox LXC, Debian 13, 4 cores of an i5-8500T shared with other guests, 6 GB, consumer NVMe |
| OS | macOS 27.0.1 | Debian 13 |
| JDK | Temurin 25.0.3 | OpenJDK 25 (SDKMAN) |
| `wal_sync_method` | `open_datasync` (default), and runs with `fsync_writethrough` | `fdatasync` (default) |

Both: Postgres 17.12, embedded (zonky binaries), on the same machine as the client. `fsync`,
`synchronous_commit` and `full_page_writes` on; `shared_buffers` 1GB, `max_wal_size` 8GB,
`checkpoint_timeout` 15min, `commit_delay` 0, `max_connections` 400, `wal_level` replica. pgjdbc
42.7.14, HikariCP.

macOS `fsync` doesn't flush the drive's write cache, so the Mac's default numbers are what a
machine with a battery-backed or no-flush cache would see. `fsync_writethrough` makes Postgres
use `F_FULLFSYNC`, a real flush, at about 9 ms per commit here. Compare devbox numbers only with
devbox numbers.

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
   One engine node consumes (LISTEN on, default batches, 1 s poll so the baseline still drains), or,
   in separate runs, nothing consumes. 3 s warm-up, then 15 s measured; three rounds with a
   consumer, one without. Wait events were sampled every 5 ms from `pg_stat_activity`, joined with
   `pg_locks` for ungranted locks, for the producers and the engine node separately. WAL flushes
   come from `pg_stat_wal` (`wal_sync`, or `wal_write` with an `open_*sync` method).
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

Commits/s, with commit p99 in ms in brackets. With a consumer: the median of three rounds.

**devbox**, with a consumer:

| Producers | trigger as it is | no notify | notify only into an idle topic | debounced after commit |
|---:|---:|---:|---:|---:|
| 1 | 369 (4.5) | 485 (3.0) | 444 (2.9) | 345 (4.2) |
| 8 | 441 (29.3) | 2,479 (4.9) | 2,153 (5.4) | 2,075 (21.5) |
| 32 | 522 (76.6) | 4,273 (10.8) | 3,464 (19.0) | 3,434 (48.8) |
| 64 | 522 (161) | 4,037 (54.8) | 3,136 (54.7) | 3,851 (57.0) |
| 128 | 482 (1,688) | 4,239 (25.8) | 3,460 (55.5) | 4,203 (23.0) |

**devbox**, without a consumer:

| Producers | trigger as it is | no notify | notify only into an idle topic | debounced after commit |
|---:|---:|---:|---:|---:|
| 1 | 285 (31.1) | 447 (2.7) | 409 (4.1) | 401 (3.3) |
| 8 | 618 (18.5) | 2,364 (4.3) | 2,566 (4.0) | 2,353 (5.1) |
| 32 | 495 (857) | 3,923 (59.4) | 3,834 (49.2) | 4,504 (12.1) |
| 64 | 847 (88.6) | 5,129 (13.6) | 3,933 (22.7) | 5,060 (13.5) |
| 128 | 832 (220) | 4,894 (18.8) | 4,084 (30.7) | 4,757 (20.6) |

**Mac, real flush** (`wal_sync_method=fsync_writethrough`):

| Producers | trigger, with consumer | no notify, with consumer | debounced, with consumer | trigger, no consumer | no notify, no consumer | debounced, no consumer |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 112 (19.1) | 111 (20.0) | 107 (20.1) | 159 (19.3) | 177 (16.9) | 195 (15.8) |
| 8 | 110 (272) | 820 (22.0) | 800 (23.3) | 193 (231) | 792 (24.5) | 799 (22.2) |
| 32 | 106 (1,156) | 2,919 (22.1) | 2,882 (21.9) | 223 (385) | 2,700 (29.5) | 2,655 (25.3) |
| 64 | 113 (1,289) | 4,808 (23.4) | 4,643 (34.4) | 233 (1,156) | 4,470 (36.1) | 4,880 (25.5) |
| 128 | 102 (2,146) | 6,009 (35.1) | 5,946 (26.8) | 197 (1,317) | 6,883 (39.7) | 5,022 (53.9) |

**Mac, default** (`open_datasync`, no drive-cache flush):

| Producers | trigger, with consumer | no notify, with consumer | idle topic only, with consumer | debounced, with consumer | trigger, no consumer | no notify, no consumer | debounced, no consumer |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4,498 (1.5) | 6,494 (0.7) | 2,532 (1.1) | 5,297 (0.7) | 6,122 (0.3) | 5,742 (0.3) | 7,166 (0.2) |
| 8 | 8,254 (3.1) | 10,333 (2.8) | 8,883 (3.0) | 8,865 (2.9) | 12,339 (1.7) | 8,639 (4.2) | 12,383 (2.3) |
| 32 | 6,540 (10.4) | 22,691 (4.8) | 11,111 (5.8) | 11,282 (5.8) | 8,901 (9.2) | 14,561 (7.2) | 17,291 (7.2) |
| 64 | 4,930 (63.4) | 21,545 (11.1) | 16,349 (15.3) | 16,421 (11.4) | 7,322 (85.4) | 15,246 (11.3) | 13,353 (16.6) |
| 128 | 3,856 (47.2) | 17,636 (27.8) | 14,925 (31.9) | 15,832 (24.9) | 6,181 (133) | 10,877 (45.8) | 15,314 (30.7) |

The Mac's default settings are the noisiest setup: rounds at the same point varied by up to 2x,
and with a flush this cheap, 1 and 8 producers overlap. From 32 producers up the trigger was the
slowest in every round and every setup.

**Why the debounced notify trails "no notify" on the Mac's default settings with a consumer**
(32 and 64 producers): without a notify the consumer only polls every second and falls far behind.
It completed 5,000 to 10,000 tasks/s while the backlog reached 180,000 to 330,000, which left the
WAL and CPU to the producers. With the debounced notify the consumer keeps up (about 14,000
tasks/s, backlog near zero), and its claims and completions share the WAL flushes. Without a
consumer, the debounced notify was as fast as no notify or faster, except at 64 producers (88 %).

**Where the producers waited.** Average number of producer backends in each state, the top two:

| Setup | Producers | trigger as it is | no notify |
|---|---:|---|---|
| devbox | 8 | 6.3 `Lock:object (pg_database 0)`; 0.5 `IO:WalSync` | 3.7 `LWLock:WALWrite`; 0.8 `IO:WalSync` |
| devbox | 64 | 62.2 `Lock:object (pg_database 0)`; 0.5 `IO:WalSync` | 25.4 `LWLock:WALWrite`; 17.3 `LWLock:BufferContent` |
| devbox | 128 | 124.8 `Lock:object (pg_database 0)`; 0.5 `IO:WalSync` | 62.8 `LWLock:BufferContent`; 31.6 `LWLock:WALWrite` |
| Mac, real flush, no consumer | 64 | 62.8 `Lock:object (pg_database 0)`; 1.0 `IO:WalSync` | 43.4 `LWLock:WALWrite`; 7.8 `LWLock:BufferContent` |
| Mac, default | 64 | 57.6 `Lock:object (pg_database 0)`; 1.0 `LWLock:BufferContent` | 25.2 `LWLock:WALWrite`; 7.5 `LWLock:BufferContent` |

Without the notify, producers wait in `LWLock:WALWrite`, which is group commit working: one
backend flushes for everyone queued behind it. Commits per flush at 64 producers without a
consumer: 1.0 with the trigger on every machine; without the notify 11.2 on devbox, 23.8 on the Mac
with a real flush and 3.7 on its default settings. With a consumer the trigger's figure drops to
0.6 to 0.9, because the consumer's commits flush too.

### Throughput ceiling

Sustained tasks/s: the paced rate held, and what completed at it.

| | Nodes | Batches per node | devbox, trigger | devbox, debounced | Mac, trigger | Mac, debounced |
|---|---:|---:|---:|---:|---:|---:|
| 64 producers × 1 task | 1 | 2 | 328 | 3,132 | 4,044 | 5,489 |
| 64 producers × 1 task | 1 | 8 | 387 | 3,414 | 2,907 | 6,249 |
| 64 producers × 1 task | 3 | 2 | 341 | 3,651 | 2,737 | 10,137 |
| 64 producers × 1 task | 3 | 8 | 413 | 3,385 | 3,307 | 9,336 |
| 8 producers × 100 tasks | 1 | 2 | 7,660 | 6,273 | 15,466 | 15,451 |
| 8 producers × 100 tasks | 1 | 8 | 8,907 | 3,693 | 17,955 | 20,953 |
| 8 producers × 100 tasks | 3 | 2 | 11,647 | 8,825 | 43,359 | 35,666 |
| 8 producers × 100 tasks | 3 | 8 | 14,213 | 12,000 | 36,891 | 33,209 |

Flat out, the batched producers pushed 21,000 to 47,000 tasks/s on devbox and 78,000 to 120,000
on the Mac, so the consumers were the limit there.

- **One task per transaction:** with the trigger, the notify lock is the ceiling, and more nodes
  don't help. With the debounced notify it was 8 to 11 times higher on devbox, and 1.4 to 3.7
  times on the Mac. The consumers then set the limit.
- **100 tasks per transaction:** a push transaction is one commit for 100 tasks, so the notify lock
  hardly matters. The debounced column differs from the trigger's in both directions. That is the
  method's noise: the paced rate is 90 % of a flat-out run, and the flat-out runs load the shared CPU
  differently from one run to the next. Read these rows as "the trigger doesn't limit batched
  pushes", not as one variant beating the other.
- **Three nodes** gave 1.5 to 2.8 times one node's rate with batched pushes and the trigger.
  Everything ran on one machine (4 cores on devbox), so a deployment with nodes on their own
  machines should scale further.

### End-to-end latency

From push commit to handler start, ms. One run each; LISTEN off ran 120 s, the others 30 s.

| Wake-up | Rate/s | Mac p50 | Mac p99 | Mac max | devbox p50 | devbox p99 | devbox max |
|---|---:|---:|---:|---:|---:|---:|---:|
| trigger as it is, LISTEN on | 100 | 2.2 | 7.2 | 72 | 5.8 | 9.5 | 22 |
| debounced after commit, LISTEN on | 100 | 3.8 | 11.8 | 13 | 5.5 | 10.6 | 23 |
| notify only into an idle topic, LISTEN on | 100 | 1.9 | 3.8 | 11 | 15,153 | 29,717 | 30,015 |
| LISTEN off (poll every 30 s) | 100 | 15,023 | 29,682 | 30,011 | 15,099 | 29,692 | 30,011 |
| trigger as it is, LISTEN on | 1,000 | 1.6 | 6.0 | 77 | 4.7 * | 49.3 * | 156 * |
| debounced after commit, LISTEN on | 1,000 | 5.1 | 12.3 | 17 | 4.8 | 13.9 | 26 |
| notify only into an idle topic, LISTEN on | 1,000 | 15,363 | 29,716 | 30,007 | 16,055 | 29,698 | 30,018 |
| LISTEN off (poll every 30 s) | 1,000 | 15,353 | 29,689 | 30,008 | 7.8 * | 29,304 * | 30,002 * |

\* On devbox, with the trigger, 8 producers can't commit 1,000 pushes a second: those runs
reached about 335 (trigger) and 413 tasks/s (LISTEN off, where the trigger still runs). With
LISTEN off at that rate the queue never drained, so the worker kept claiming: p50 7.8 ms, but 20,532
of 49,549 tasks waited over a second.

- **LISTEN off** means waiting for the poll: p50 about half the 30 s interval.
- **Debounced** adds 1.6 to 3.5 ms at p50 on the Mac and stays within the 10 ms window plus a claim
  at p99. On devbox it was as fast as the trigger at p50, and faster at the tail at 1,000/s, where
  the trigger's own contention slowed it.
- **Notify only into an idle topic** loses wake-ups by design. A push that sees a ready task skips
  the notify, but that task may be one a worker is claiming right then. The worker's next claim
  can run on a snapshot from before the push committed, find nothing, and wait for the poll. At
  100 tasks/s on the Mac the queue was empty at almost every push, so it notified. At 1,000/s, and
  already at 100/s on devbox, most tasks waited for the 30 s poll.

## Recommendation

**The contention is real, from about 8 concurrent pushing transactions** on any machine whose
flush costs real time (most production servers, any cloud disk). An application that pushes one
task per request, with dozens of requests in flight, gets its database's commit rate capped near
one commit per flush (about 500/s on devbox, 110/s on the Mac with a real flush). Its commit
latency then rises with the number of waiters. It affects every transaction that pushes, not just
pgtasks', and only them: transactions that don't notify aren't serialized. A low-concurrency or
batch-pushing application won't notice.

**Fix: notify after commit, debounced, from the engine.** Drop the INSERT trigger. When a push's
transaction commits, the engine wakes its own workers for the topic (it already does for
`independently()`). It also asks a per-node notifier to send `pg_notify('pgtasks_ready', topic)` in
a short transaction of its own: at once after a quiet spell, then at most once per topic per 10 ms.
Only those short transactions queue for the notify lock, at most about 100 a second per node and
topic, and the application's commits share flushes again. The benchmark's prototype committed as
fast as no notify with a real flush on the Mac, and without a consumer on devbox. With a consumer
on devbox it reached 80 to 100 % of no notify from 8 producers up, within that setup's noise.

Trade-offs:

- **Wake-up latency:** +1.6 to 3.5 ms at p50 on the Mac, at most the debounce window (10 ms) more
  at the tail. The window could be shorter; 10 ms already cut notifications to 80 to 100 a second.
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
- **Schema:** dropping the insert trigger changes `R__notify_ready.sql`, a repeatable file, so the
  frozen V001 is untouched; the 0.1.0 copy used by the upgrade test needs refreshing with it.

**Alternatives measured or considered:**
- Notify only into an idle topic: rejected (lost wake-ups).
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
  plan, like the claim's, would close it. Not fixed here. (Seen while the schema was still named
  `tasks`; the statements are the same.)

## Caveats

- **Embedded Postgres on the client's machine.** Producers, engines and Postgres share the CPU
  (10 cores on the Mac, 4 on devbox). From 64 producers up the CPU is contended, and the
  no-notify runs show `LWLock:BufferContent` on the queue's hot index pages.
- **Short runs.** 15 s per point: three rounds with a consumer, one without. Rounds varied by up to
  2x at the same point on the Mac's default settings, and single points by up to about 2x on
  devbox. The conclusions rest on gaps of 4x to 50x, well outside that noise.
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

Options are listed in the class's Javadoc (producer counts, variants, durations, repeats, running
without a consumer, extra server settings such as
`-Dbenchmark.postgres.config=wal_sync_method=fsync_writethrough`, the schema).
