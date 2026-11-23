# Wake-ups, throughput ceiling and latency

Run on 2026-11-22 with `NotifyBenchmark` in `pgtasks-benchmark`, against the library as it ships:
pushes in a bound transaction, so the engine sends the wake-up notification after the commit
(`feee2a3`), and the completion plans fixed (`01e2ce9`, `8b2ce4e`). The real engine and schema
(`pgtasks`), **`fsync`, `synchronous_commit` and `full_page_writes` on**. devbox ran commit
`c294937`, the Mac `247f556`; they differ only in when the ceiling search stops.

The question it started from: Postgres serializes the commits of all transactions that sent a
`NOTIFY` behind one database-wide lock (`PreCommit_Notify` locks object 0 of `pg_database` and
holds it until the commit has been flushed). Does waking workers cap how fast an application with
many concurrent transactions can commit? And what are pgtasks' throughput ceiling and wake-up
latency?

## Short answer

- **Pushing doesn't serialize the application's commits.** A `tasks.push(...)` in a bound
  transaction (Spring, `JdbcTransactions`) doesn't notify inside it. After the commit the engine
  wakes its own workers, and sends one notification for the other nodes from a short transaction
  of its own, at most once every 10 ms. Medians of three rounds, 8 to 128 producers, one push per
  transaction:
  - devbox (real flush): 2,387 to 4,261 commits/s, against 2,359 to 4,342 with no notification at
    all.
  - Mac with a real flush (`fsync_writethrough`): 1,037 to 9,650, against 1,102 to 7,360.
  - Mac on its default settings, where macOS doesn't flush the drive's cache: 14,220 to 23,829,
    against 14,418 to 24,785.
  - About 70 to 100 notifications a second per node, whatever the push rate.
- **Notifying inside the transaction still caps it,** and that is what `tasks.on(connection)` and
  plain-SQL inserts do (the queue's insert trigger): about one pushing commit per WAL flush from 8
  producers up. devbox 502 to 525 commits/s, Mac with a real flush 123 to 135, with commit p99
  over 1 s at 128 producers. Up to 125 of 128 producer backends waited on `Lock:object` for
  `pg_database` 0. These are the "before" numbers: until `feee2a3` every push did this.
- **Throughput ceiling** (stepped search: the offered rate raised until the backlog grows, the
  last rate held):
  - One task per transaction, 64 producers: the producers are the limit, not the engine. devbox
    3,100 to 3,500 tasks/s, Mac 14,000 to 16,000, with 1 or 3 nodes. Before, the notify lock held
    this at 328 to 413 tasks/s on devbox and 2,737 to 4,044 on the Mac.
  - 100 tasks per transaction, 8 producers: the consumers are the limit. devbox 9,100 (one node,
    2 batches) to 13,000 (8 batches) on one node, 17,000 to 18,000 on three; Mac 18,000 to 41,000
    on one node, 52,000 to 61,000 on three.
- **Wake-up latency, p50 / p99** from the push's commit to the handler starting:
  - pushed on the node that handles it: Mac 0.4 to 1.1 / 2.0 to 2.9 ms, devbox 2.8 to 5.0 /
    5.3 to 34 ms. The node wakes its own workers directly, with or without LISTEN.
  - pushed on another node, LISTEN on: about 5 / 12 ms on both machines, the 10 ms debounce
    window included.
  - pushed on another node, LISTEN off: half the 30 s poll interval at p50, the whole of it at p99.

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
devbox numbers. The Mac had other work running (load average about 4 before the runs).

## Workload

Each run gets a fresh database copied from a template: the schema, an application table
`app.event`, and 200,000 finished tasks in the topic, analyzed. A `CHECKPOINT` runs before each
run.

**Wake-up variants:**

| Variant | What it does |
|---|---|
| notify after commit (shipped) | `JdbcTransactions.inTransaction` inserts the app row and calls `tasks.push(...)`. The insert trigger skips the push; after the commit the engine wakes its workers and its `ReadyNotices` sends `pg_notify` from its own transaction, at once after a quiet spell, then at most once per 10 ms |
| notify at commit (trigger) | the app row and `tasks.on(connection).push(...)` on one connection: the insert trigger notifies inside the transaction. Every push before `feee2a3`; pushes on an explicit connection still |
| no notify (baseline) | as the trigger variant, with the insert trigger dropped: nothing notifies |

**The four parts:**

1. **Producer contention:** N producers (1, 8, 32, 64, 128), each committing transactions that
   insert one row into `app.event` and push one task. One engine node consumes (LISTEN on,
   default batches, 1 s poll so the baseline still drains), or, in separate runs, nothing consumes.
   Pushes go through that node. 3 s warm-up, then 15 s measured; three rounds with a consumer, one
   without. Wait events sampled every 5 ms from `pg_stat_activity`, joined with `pg_locks` for
   ungranted locks. WAL flushes from `pg_stat_wal` (`wal_sync`, or `wal_write` with an
   `open_*sync` method).
2. **Throughput ceiling, flat out then paced:** producers push flat out while 1 or 3 engine nodes
   claim and complete (no-op handler, batches of 100, 2 or 8 batches per node). The nodes are
   **separate engines in one JVM**, each with its own pool, sharing the machine with Postgres; the
   producers push through node 1. Then a fresh run pushes at 90 % of the lower of what was pushed
   and completed. It held if the producers pushed at least 97 % of that and the backlog grew by
   less than a second's worth. Two shapes: 64 producers with one task per transaction, and 8 with
   100.
3. **Stepped ceiling search:** the same shapes and nodes. From 60 % of the flat-out guess, the rate
   goes up by half while it holds, then the gap between the last rate held and the first that
   didn't is halved three times. A step held if the producers pushed at least 97 % of the rate
   and the backlog grew by less than a quarter of a second's worth, or than the tasks the nodes
   can hold claimed at once (nodes × batches × 100), whichever is more. It stops early if the
   next step is above what the producers pushed flat out: the producers are then the limit.
4. **End-to-end latency:** 8 producers at a steady 100 or 1,000 tasks/s in total, one task per
   transaction, one engine node with default settings (2 batches of 100, 30 s poll). Pushed
   through the consuming node, or through a second engine that only pushes. Latency is from the
   push's commit returning to the handler starting.

## Results

### Producer contention

Commits/s, with commit p99 in ms in brackets. With a consumer: the median of three rounds.

**devbox:**

| Producers | shipped, with consumer | trigger, with consumer | no notify, with consumer | shipped, no consumer | trigger, no consumer | no notify, no consumer |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 410 (3.6) | 294 (22.6) | 406 (4.0) | 483 (2.5) | 498 (2.6) | 546 (2.2) |
| 8 | 2,387 (5.2) | 509 (22.5) | 2,359 (7.7) | 2,433 (5.1) | 661 (17.3) | 2,122 (10.3) |
| 32 | 4,075 (12.0) | 511 (85.9) | 4,216 (11.0) | 4,466 (28.6) | 682 (56.2) | 3,893 (44.2) |
| 64 | 4,261 (17.0) | 525 (152) | 4,342 (17.1) | 4,896 (14.6) | 835 (95.5) | 5,175 (14.0) |
| 128 | 3,994 (30.7) | 502 (1,009) | 4,312 (24.3) | 4,707 (20.2) | 761 (1,030) | 4,513 (26.9) |

**Mac, real flush** (`wal_sync_method=fsync_writethrough`):

| Producers | shipped, with consumer | trigger, with consumer | no notify, with consumer | shipped, no consumer | trigger, no consumer | no notify, no consumer |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 123 (11.7) | 123 (12.6) | 136 (9.5) | 240 (7.6) | 238 (5.1) | 233 (5.2) |
| 8 | 1,037 (15.1) | 134 (136) | 1,102 (12.3) | 1,161 (9.7) | 274 (38.2) | 1,060 (11.5) |
| 32 | 3,985 (16.4) | 135 (1,074) | 3,206 (17.5) | 4,539 (11.4) | 260 (361) | 4,077 (16.1) |
| 64 | 6,633 (16.8) | 131 (745) | 6,941 (17.6) | 7,143 (16.7) | 229 (1,121) | 5,738 (12.4) |
| 128 | 9,650 (23.4) | 123 (1,130) | 7,360 (18.8) | 7,346 (14.8) | 229 (1,502) | 6,725 (24.1) |

**Mac, default** (`open_datasync`, no drive-cache flush):

| Producers | shipped, with consumer | trigger, with consumer | no notify, with consumer | shipped, no consumer | trigger, no consumer | no notify, no consumer |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,759 (3.1) | 2,612 (3.1) | 7,454 (0.1) | 8,180 (0.0) | 7,824 (0.1) | 8,324 (0.1) |
| 8 | 14,220 (1.7) | 10,449 (2.3) | 14,418 (2.5) | 24,046 (0.2) | 11,147 (2.9) | 15,037 (1.9) |
| 32 | 21,227 (5.0) | 8,796 (9.8) | 24,785 (4.4) | 20,464 (4.9) | 13,962 (5.8) | 20,031 (5.4) |
| 64 | 23,829 (13.4) | 12,663 (7.3) | 24,299 (12.1) | 18,344 (11.1) | 8,421 (55.6) | 19,562 (9.6) |
| 128 | 21,369 (31.0) | 10,863 (17.3) | 19,784 (30.2) | 17,457 (25.8) | 8,375 (109) | 17,523 (26.0) |

- **Shipped and no notify are within each other's noise everywhere from 8 producers up.** The
  rounds varied by up to 2x at one point on the Mac, and by up to 20 % on devbox; shipped was
  ahead as often as behind.
- **One producer pays one flush per commit anyway.** On the Mac's default settings, the
  one-producer rounds with a consumer were the noisiest of all: 6,462, 1,849 and 2,759 commits/s
  for shipped, and 7,454, 7,504 and 2,922 for no notify, minutes apart. Without a consumer the
  three variants were equal.
- **With 64 and 128 producers one consumer node falls behind** (devbox 2,600 to 3,600 tasks/s
  completed, Mac default 2,400 to 8,000), shipped and no notify alike: the backlog grew by 10,000
  to 350,000 in 15 s. That is the consumer's limit, which the ceiling section measures, not the
  wake-up's. With the trigger the consumer keeps up, because the producers are held back.

**Where the producers waited.** Average number of producer backends in each state, the top two,
and commits per WAL flush:

| Setup | Producers | shipped | trigger | no notify |
|---|---:|---|---|---|
| devbox | 8 | 3.6 `LWLock:WALWrite`; 0.8 `IO:WalSync` · 3.2 per flush | 6.3 `Lock:object (pg_database 0)`; 0.5 `IO:WalSync` · 0.7 | 3.8 `LWLock:WALWrite`; 0.8 `IO:WalSync` · 3.4 |
| devbox | 64 | 23.4 `LWLock:BufferContent`; 14.8 `LWLock:WALWrite` · 10.2 | 62.2 `Lock:object (pg_database 0)`; 0.5 `IO:WalSync` · 0.7 | 29.1 `LWLock:WALWrite`; 14.6 `LWLock:BufferContent` · 11.4 |
| devbox | 128 | 64.9 `LWLock:BufferContent`; 30.8 `LWLock:WALWrite` · 11.6 | 124.8 `Lock:object (pg_database 0)`; 0.5 `IO:WalSync` · 0.7 | 71.8 `LWLock:BufferContent`; 18.7 `LWLock:WALWrite` · 11.9 |
| Mac, real flush | 64 | 41.1 `LWLock:WALWrite`; 8.7 `LWLock:BufferContent` · 22.5 | 62.8 `Lock:object (pg_database 0)`; 0.5 `LWLock:WALWrite` · 0.5 | 40.1 `LWLock:WALWrite`; 7.8 `LWLock:BufferContent` · 23.1 |
| Mac, real flush | 128 | 55.2 `LWLock:WALWrite`; 48.6 `LWLock:BufferContent` · 28.4 | 124.0 `Lock:object (pg_database 0)`; 0.5 `LWLock:WALWrite` · 0.5 | 55.7 `LWLock:WALWrite`; 45.1 `LWLock:BufferContent` · 29.2 |

(First round of three; the others look the same.) Shipped waits like no notify: in
`LWLock:WALWrite`, which is group commit working (one backend flushes for everyone queued behind
it), and from 64 producers on the queue's hot index pages (`LWLock:BufferContent`). The trigger
queues for the notify lock and gets under one commit per flush, because the consumer's commits
flush too; without a consumer exactly 1.0 on every machine.

### Throughput ceiling

**Stepped search,** shipped code, tasks/s. "Sustained" is the last rate held; "(producers)" means
the producers couldn't push the next step, so the engine's ceiling is higher.

| Shape | Nodes | Batches per node | devbox: flat out pushed / completed | devbox sustained | Mac: flat out pushed / completed | Mac sustained |
|---|---:|---:|---:|---:|---:|---:|
| 64 producers × 1 task | 1 | 2 | 4,270 / 3,493 | 3,100 (producers) | 20,295 / 5,385 | 15,000 (producers) |
| 64 producers × 1 task | 1 | 8 | 4,060 / 3,800 | 3,400 (producers) | 19,018 / 7,837 | 15,000 (producers) |
| 64 producers × 1 task | 3 | 2 | 3,924 / 3,922 | 3,500 (producers) | 16,813 / 12,877 | 16,000 (producers) |
| 64 producers × 1 task | 3 | 8 | 3,851 / 3,843 | 3,200 | 17,361 / 16,556 | 14,000 (producers) |
| 8 producers × 100 tasks | 1 | 2 | 40,653 / 5,067 | 9,100 | 141,799 / 24,772 | 18,000 |
| 8 producers × 100 tasks | 1 | 8 | 40,506 / 6,587 | 13,000 * | 137,416 / 23,119 | 41,000 |
| 8 producers × 100 tasks | 3 | 2 | 36,106 / 12,073 | 18,000 | 106,925 / 39,823 | 52,000 |
| 8 producers × 100 tasks | 3 | 8 | 27,333 / 10,893 | 17,000 | 104,581 / 46,059 | 61,000 |

\* 13,000 held, then failed when the search rounded back onto it: within the noise of the step.
From `247f556` the search stops instead of retrying a rate.

The steps, for the record: devbox 3 nodes × 2 batches, 100 per transaction: 7,243 ✓, 10,000 ✓,
15,000 ✓, 22,000 ✗ (backlog +111,000 in 15 s), 18,000 ✓, 20,000 ✗, 19,000 ✗. Mac 3 nodes × 8
batches: 27,635 ✓, 41,000 ✓, 61,000 ✓, 91,000 ✗, 76,000 ✗, 68,000 ✗, 64,000 ✗. The full lists
are in the runs' `notify-results.md`.

- **One task per transaction:** with the notification after the commit, 64 producers commit
  about as fast as the machine flushes WAL for them (devbox about 4,000 a second, the Mac on its
  default settings about 20,000), and the engine keeps up with that.
- **Flat out understates the ceiling.** Producers pushing without limit take the CPU and the WAL
  from the consumers: on the Mac, one node completed 5,385 tasks/s flat out but sustained 15,000
  when the push rate was set. The search measures what a node sustains; the flat-out columns are
  where it started.
- **100 tasks per transaction:** the producers push 27,000 to 142,000 tasks/s and the consumers
  are the limit. More batches per node help on the Mac (one node 18,000 with 2, 41,000 with 8) and
  less on devbox's 4 shared cores. Three nodes in one JVM, on the same machine as Postgres, gave
  1.3 to 2.9 times one node.

**Flat out, then paced at 90 %** (the method of the first run), shipped against the trigger:

| Shape | Nodes | Batches per node | devbox shipped | devbox trigger | Mac shipped | Mac trigger |
|---|---:|---:|---:|---:|---:|---:|
| 64 × 1 | 1 | 2 | 2,885 | 472 | 3,416 | 5,630 |
| 64 × 1 | 1 | 8 | 3,471 | 361 | 1,415 | 5,840 |
| 64 × 1 | 3 | 2 | 3,432 | 277 | 8,218 | 9,190 |
| 64 × 1 | 3 | 8 | 3,444 | 397 | 11,632 | 7,988 (not held) |
| 8 × 100 | 1 | 2 | 4,087 | 7,007 | 22,526 (not held) | 18,388 |
| 8 × 100 | 1 | 8 | 6,093 | 8,167 | 25,491 | 23,206 |
| 8 × 100 | 3 | 2 | 9,420 | 12,946 | 38,620 (not held) | 39,294 |
| 8 × 100 | 3 | 8 | 11,340 | 13,984 | 35,069 | 45,272 |

Tasks/s completed at the paced rate. On devbox the trigger caps one task per transaction at 277
to 472 tasks/s; shipped is 6 to 12 times that. Elsewhere the 90 % method is unreliable: with
shipped pushes the producers aren't held back by a lock, so a flat-out run starves the
consumers, and 90 % of what they completed then is a low guess. That is why the trigger can look
faster here (Mac 64 × 1, one node: 5,630 against 3,416) while the search finds shipped sustaining
15,000 on the same setup. Read the stepped search for the ceiling.

### End-to-end latency

From push commit to handler start, ms. One run each, 30 s; poll-only runs 120 s.

| Wake-up | Pushed on | LISTEN | Rate/s | Mac p50 | Mac p99 | Mac max | devbox p50 | devbox p99 | devbox max |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| shipped | the consuming node | on | 100 | 1.1 | 2.0 | 12 | 2.8 | 5.3 | 19 |
| shipped | another node | on | 100 | 4.9 | 12.2 | 14 | 5.1 | 12.7 | 22 |
| shipped | the consuming node | off | 100 | 1.2 | 2.2 | 4 | 4.6 | 49.1 | 154 |
| shipped | another node | off | 100 | 15,039 | 29,701 | 30,007 | 15,124 | 29,714 | 30,013 |
| trigger (before) | another node | on | 100 | 2.8 | 4.2 | 29 | 4.9 | 10.9 | 2,330 |
| shipped | the consuming node | on | 1,000 | 0.4 | 2.9 | 55 | 5.0 | 34.2 | 608 |
| shipped | another node | on | 1,000 | 4.8 | 12.5 | 36 | 4.5 | 12.2 | 308 |
| shipped | the consuming node | off | 1,000 | 0.4 | 2.6 | 41 | 5.1 | 13.8 | 26 |
| shipped | another node | off | 1,000 | 15,383 | 29,721 | 30,008 | 5.8 * | 26,824 * | 29,994 * |
| trigger (before) | another node | on | 1,000 | 1.6 | 6.0 | 29 | 5.5 ** | 10.7 ** | 21 ** |

\* On devbox at 1,000/s the queue never emptied, so the worker kept claiming without waiting for
the poll: p50 5.8 ms, but 31,496 of 115,592 tasks waited over a second.
\** On devbox the trigger let 8 producers push only about 400 tasks/s (12,021 in 30 s), not 1,000.

- **Pushed on the consuming node,** the engine wakes its workers directly after the commit:
  about 1 ms at p50 on the Mac, LISTEN on or off.
- **Pushed on another node,** the wake-up is the debounced notification: about 5 ms at p50, 12 ms
  at p99, within the 10 ms window plus a claim. The trigger notified at every commit, so it was
  2.1 to 3.2 ms faster at p50 on the Mac, and about as fast on devbox.
- **LISTEN off** means waiting for the poll: p50 about half the 30 s interval.

## Before: the notification inside every pushing transaction

The first run of this benchmark (at `9ce3778`, the same day) measured the library when every push
notified from the insert trigger, inside the application's transaction. It led to `feee2a3`.
Those numbers, for comparison with the tables above (medians of three rounds):

- devbox: 441 to 522 commits/s from 8 to 128 producers with the trigger, against 2,479 to 4,273
  without the notification; Mac with a real flush 102 to 113 against 820 to 6,009.
- Throughput ceiling, 90 % method: one task per transaction devbox 328 to 413 tasks/s, Mac 2,737
  to 4,044, whatever the number of nodes. 100 per transaction: one node devbox 7,660 to 8,907,
  Mac 15,466 to 17,955; three nodes devbox 11,647 to 14,213, Mac 36,891 to 43,359.
- Rejected on the way: notifying only into an idle topic lost wake-ups (p50 15 s at 1,000
  tasks/s). A prototype of the debounced notification inside the benchmark matched the
  no-notify baseline and cost 1.6 to 3.5 ms of p50 latency; the engine's `ReadyNotices` measured
  above is its shipped form.

The trigger is still there as the fallback for pushes the engine doesn't see commit, which is
why the trigger variant above still measures something real: `tasks.on(connection)` and plain-SQL
inserts. See [Operations, "Wake-ups and the notify lock"](../operations.md#wake-ups-and-the-notify-lock).

## Found on the way

- **Fixed: completion walked the topic's history while `pgtasks.task` had no statistics.** The
  first run found the completion's UPDATE using `task_completed_idx` by topic alone and reading
  every task of the topic per batch, the consumer falling to 50 to 100 tasks/s. The cause was
  partly cached generic plans: fixed in `01e2ce9` (task rows reached by primary key, no cached
  generic plans) and `8b2ce4e` (history lookups planned for an average topic). These runs keep
  the 200,000 analyzed history rows only to stay comparable with the first run.
- **Fixed: a task table analyzed while empty.** Statistics taken right after retention emptied
  `pgtasks.task` say it has pages but no rows; claims, completions and lookups then read every
  task until the next analyze. `1861080` has the store analyze the table at start and with the
  leader's count snapshot when its statistics say empty and it isn't.
- **The 90 % paced method understates the ceiling** once producers aren't held back by the
  notify lock (see above), so this run adds the stepped search.

## Caveats

- **Embedded Postgres on the client's machine.** Producers, engines and Postgres share the CPU
  (10 cores on the Mac, 4 on devbox). From 64 producers up the CPU is contended, and every
  variant shows `LWLock:BufferContent` on the queue's hot index pages.
- **Short runs.** 15 s per point; three rounds with a consumer, one without. Rounds varied by up
  to 3.5x at one point on the Mac's default settings (one producer), and up to 20 % on devbox
  from 8 producers up (45 % once at one producer). The Mac had other
  sessions' work running. The conclusions rest on gaps of 4x to 80x (trigger against the others),
  and on shipped and no notify overlapping.
- **Engines in one JVM.** The three "nodes" are separate engines with separate pools, LISTEN
  connections and node rows, in one process.
- **A no-op handler** and a 7-byte payload: the ceiling is the library's and the database's, not
  an application's.
- **Postgres 17.** The notify lock is the same in every supported version.

To rerun:

```
mvn -Pbenchmark -pl pgtasks-benchmark -am test -Dtest=NotifyBenchmark -Dsurefire.failIfNoSpecifiedTests=false -Dbenchmark.notify=all
```

Options are listed in the class's Javadoc (parts, producer counts, variants for each part,
batches, durations, repeats, running without a consumer, extra server settings such as
`-Dbenchmark.postgres.config=wal_sync_method=fsync_writethrough`, the schema).
