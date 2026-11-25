# Findings from running the order service on pgtasks

What a team would hit using pgtasks for an order service in production, found by building the
app in this directory and running it on Postgres with three instances, load and chaos. The list
is ranked by severity within each section. Library commits referred to are on branch `app/trial`.

> **A record of the run on 2026-11-22, before the core was trimmed.** Several features it used
> were removed afterwards: cluster-wide rate limits, circuit breakers, pausing topics, recurring
> tasks, junctions, and `Decision.throttled`/`unavailable`. The app now does without them (see
> the README). Where each finding stands today:
>
> | Finding | Now |
> |---|---|
> | 1, 3, 4, 5, 11, 15, 16 | Gone with the feature they were about |
> | 2 | Fixed (`89fd677`) |
> | 6 | Suspended tasks are released with `admin().release(...)`, the endpoint or the dashboard |
> | 7 | Nodes are named `<application>@<host>`, and a graceful stop removes the node's row |
> | 8, 20 | `pgtasks.heartbeat` and `pgtasks.node-expiry`; operations.md says what a `kill -9` costs |
> | 9, 14, 18, 19 | Documented in testing.md and operations.md |
> | 13 | The endpoint returns an error as an object |
> | 17 | patterns.md's fan-in example was rewritten around `later()` |
> | 10, 12 | Open |

## The runs

| | Calibration | Long run |
|---|---|---|
| pgtasks | `f716caa` (main) | `4d5e8d4` (`f716caa` + the fan-in fix) |
| Load | 2 min, 40 orders/s, burst of 300/s for 2 s every 60 s | 32 min, 40 orders/s, burst of 300/s for 2 s every 120 s, 2 % resent request ids |
| Setup | Postgres 17 in Docker (colima, 2 vCPU, 2 GB), 3 JVMs on an M4 Mac, partner stub with 2 % 500s, 1 % 429s, 0.5 % lost replies, 1 % 3 s answers (timeout 2 s), 3 % declined payments, 1 % rejected addresses | same |

Chaos during the long run (`scripts/chaos.sh`, minutes from start): kill -9 an instance (3), restart it (4),
restart Postgres (7), every partner call fails with 500 (10-13), partners refuse connections (15-17), pause
`orders.ship` (19-21), rolling restart of all three instances with `orders.payment` concurrency 4 and batch
size 16 (23-25; it also deployed an app fix, see the appendix), stop rejecting addresses (29). Afterwards,
override the `carrier` limit through the endpoint, and revive the dead letters by error.

**Results of the long run** (`scripts/verify.sh`, after the queues drained):

- 86,407 requests, 84,654 orders created, 1,753 repeated request ids answered 200 with the first order, 0 given
  up by the client. 1,722 requests were refused by a dead or restarting instance and resent to another, and 7
  got a 500 during the Postgres restart.
- API latency (POST /orders, three inserts and three pushes in one transaction): p50 4-7 ms, p99 11-25 ms in
  every 10 s window except three: 853 ms during the Postgres restart, then 248 and 154 ms.
- Every order ended in a final state: 82,135 COMPLETED (paid, reserved, shipped, notified), 2,519 CANCELLED
  (declined, stock released, customer told, never charged or shipped). 0 in flight, stock bookkeeping exact,
  0 notifications recorded twice on our side.
- Partner side effects: exactly one idempotency key with a success per order and endpoint (84,654 payment
  keys; 0 payments for declined orders; 82,135 shipments; 166,789 notifications). A key that succeeded more than
  once (the same task calling again after a lost reply, a timeout, a dead node or a failed completion):
  1,233 payments (1.5 %), 1,227 shipments, 2,509 notifications. That matches the stub's rates of lost replies
  and slow answers, and is what the docs say can happen. The idempotency key covers it.
- About 671,000 tasks completed in 40 minutes (avg ~280/s; peaks above 400/s while draining).
- 466 shipments dead-lettered (the carrier's 422s), revived by error type after the "fix", all shipped within 15 s.
- The recurring reconcile ran once in 40 of 41 minutes, and twice in one (finding 3).
- 38 claims of the killed instance were requeued 2 min 3 s after the kill (expiry set to 2 min, see finding 8).
- Pause reached all three nodes within 5 ms. About 90 tasks finished afterwards, from batches already
  running (as documented). Resume worked.
- Breakers: opened on every node within about 30 s of each partner outage. After the first outage they closed
  over the next two probe intervals, after the second within 20 s.
- Postgres restart: notification connections reconnected, polls and completions retried, the queues kept going.

## Bugs

### 1. Fan-in loses groups when parts complete at the same time (Postgres). Fixed: `4d5e8d4`

**What happened.** The calibration run left 55 of 5,884 orders (0.9 %) paid, reserved and never shipped. Their
ship task stayed suspended for good. For each of them both parts had completed, 2-25 ms apart, on different
nodes, and both markers were in the target topic. Each part's completion checked the group in its own
transaction, before the other part had committed, so neither saw a complete group. Nothing ever looks at the
group again, and no operator action can release a suspended task (finding 6). The in-memory store runs one
transaction at a time, which is why the engine tests and `JunctionTck` never saw this.

**Repro.** `JunctionTck.parts_completing_concurrently_still_release_the_group` and
`suspended_parts_completing_concurrently_still_release_the_group`: two store transactions, each pushes its part
and stages the junction, and the first commits while the second is open. They fail on Postgres (FRESH and
DRAINED) at `f716caa` and pass on memory.

**Fix.** Before the junction check, each completion takes a transaction-level advisory lock per group member
(`pg_advisory_xact_lock(topic_id, hashtext(identifier))`, sorted, in a statement of its own). The later
completion waits for the earlier one to commit, and its check (the next statement, so a new snapshot) sees the
other part. The long run lost 0 of 84,654. None of the run's deadlocks involved these locks.

**Left to improve.** The lock is held until the completion commits, which includes every resource's
`beforeCommit`. In the long run, completions waited up to 9.8 s for the lock behind completions that were stuck in
the app's own row-lock deadlocks. Applying junctions after `beforeCommit`, as the last step
before commit, would shorten that.

### 2. A completion cut short by stop on a transient error counts as a failure. Fixed: `89fd677`

**What happened.** During the rolling restart, each stopping instance had a stock batch of 50 whose completion
was retrying a deadlock. `transactRetrying` gives up on transient errors once the node is stopping, and
`BatchRun` then recorded every task of the batch as failed: 150 counted attempts in total. The stopping node's
breaker for `orders.stock` opened ("Most recent tasks in orders.stock fail"). With `Retry.none()` or a few
attempts, these would have been dead letters, for a deadlock. The docs say shutdown hands back what is left
without counting an attempt.

**Repro.** `EngineTest.stop_hands_back_a_batch_whose_completion_keeps_hitting_a_transient_error`: a resource
whose `beforeCommit` throws SQLState 40P01, then `stop()`. At `f716caa` the task has attempt 1 and one failure.

**Fix.** When the completion fails on a transient error and the node is stopping, release the claims as a batch
that stop cuts short (no attempt, no failure, the breaker doesn't see it).

### 3. A recurring task fires twice when the node's clock is slightly behind the database's. Fixed: `da630b1`

**What happened.** The reconcile task ran twice for 18:40: pushed at 18:40:00.002 and 18:40:01.012 by the same
leader. `dueSchedules` picks schedules by the database's `clock_timestamp()`, but `fireSchedules` computes the
next run from the JVM's clock. With the JVM a few milliseconds behind (colima's VM clock against the Mac's), the
next run after the 18:40:00 slot was 18:40:00 again, due at once. It only happens when the fire lands within the
skew. Of 41 slots, the one pushed 2 ms after the minute doubled and the one pushed 37 ms after didn't.

**Repro.** `RecurringTest.fires_once_when_the_node_clock_lags_the_store`: a store whose `clock()` is 10 ms behind
its own idea of now.

**Fix.** Compute the next run from the later of now and the run just fired.

### 4. A rate-limited topic claims one batch at a time across the cluster, and the metric doesn't show it

**What happened.** During the load, `orders.ship` (limit `carrier`, 150/s) completed about 75-80 tasks a second
while 120/s arrived, and its backlog grew to 103,000. `pgtasks.limit.wait` showed almost nothing (3 waits,
15 ms in total). Sampling `pg_stat_activity`: the claim holds the limit's `rate_admission` row `FOR UPDATE` while
it runs the poll, so every other claim for the same limit, on every node, queues on that row (97 samples waiting
on it against 61 running claims). On a loaded database (Postgres at 180-190 % of its 2 vCPUs) a claim took
200-320 ms instead of 12 ms, and the limited topic's ceiling became batch size / claim time ≈ 32 / 0.25 s ≈
128/s, well under the configured rate. Once the load stopped, it drained at 145/s (the limit), and at 414/s after
raising the limit to 450/s.

**Expected.** A rate limit caps the rate. It shouldn't also make claims for the limited topics single-file
across the cluster, and time spent waiting for the limit should show in `pgtasks.limit.wait`.

**Suggested fix.** Take the tokens in a short statement of their own (`UPDATE rate_admission ... RETURNING`
granted), poll outside that lock, and give back what the poll didn't use. Or at least count the lock wait in
`pgtasks.limit.wait`, and say in the docs that claims for one limit serialize.

## Surprising behaviour and missing features

### 5. Fan-in needs marker tasks in the target topic, which use up its rate limit

The junction counts tasks in the target topic and adds the staging task's own identifier to the group. To ship
one order once both payment and stock are done, the app pushes a suspended `<id>` task to `orders.ship` at order
time, and each part pushes a marker task (`<id>:paid`, `<id>:reserved`) there and stages
`junction(SHIP, ANY, {paid, reserved})`. The markers are real tasks of the ship topic: they're claimed, run the
handler (which filters them), and take `carrier` permits. Two thirds of everything the carrier limit admitted
were markers that never called the carrier (166,789 markers against 84,654 orders), so the limit had to be set
3x too high to mean what it says. With `Counts.SUSPENDED` every member is released and runs, so a 2-part fan-in
runs the aggregate handler twice.

**Suggested.** A junction that releases one named aggregate task (`part.junction(SHIP, orderId, Set.of("paid", "reserved"))`),
with the parts recorded as group rows rather than tasks of the target topic. Or let a limit count only
tasks a handler marks as using it.

### 6. No way to release a suspended task

`concepts.md` says a suspended task "waits until a junction or an operator releases it". Neither the endpoint,
the dashboard nor `TaskAdmin` can release one. The 55 orders of finding 1 could only be repaired with a REPLACE
push from code. **Suggested:** `admin().release(topic, ids)` and a button next to suspended tasks.

### 7. Every instance has the same node name, and stopped nodes stay listed for 3 days

The default node name is `spring.application.name`, so all three instances (and every pod of a deployment) are
`orders` on the dashboard. After the run it listed 16 nodes, 13 of them "late", all called `orders`: a stopped
node isn't deregistered, and `deleteDeadNodes` only forgets it after 3 days. An operator can't tell a pod that
died from one that was replaced in a deploy. **Suggested:** default the name to `<application>@<host>` (pod name
in Kubernetes), delete the node row on graceful stop (its claims are released by then), and show the host.

### 8. How long a dead node's tasks wait isn't configurable in the starter

A kill -9 holds the node's claims until it counts as dead: 10 minutes by default. Under `SINGULAR_BY_IDENTIFIER`
it also holds every later task for those identifiers (here: notifications for 32 customers). The run used 2
minutes, which took a `TaskGlobals` bean (`globals.heartbeat(5s, 2m)`): there is no
`pgtasks.heartbeat`/`pgtasks.node-expiry` property, and operations.md mentions the 10 minutes only in passing.
**Suggested:** properties for both, and a sentence in "Deploys and shutdown" on what kill -9 costs.

### 9. `TaskTester.drain(topic)` runs every topic

`OrderFlowTest` wanted to check that shipping waits for stock. `drain(PAYMENT)` also ran stock, ship and notify,
so the shipment had already happened. testing.md says drain "runs a topic until nothing in it can run", which
reads as only that topic. The test pauses the stock topic instead. **Suggested:** say so in the docs, or make
`drain(topic)` pause the others while it runs.

### 10. Batch resources with JDBC effects can't run on the in-memory store

patterns.md recommends a `BatchResource`'s `beforeCommit` for writes to the same database, and testing.md
recommends the in-memory engine for flow tests. The two don't meet: on the memory store the connection throws
when used, so every handler that stages SQL fails. `OrderFlowTest` subclasses the resource to record the SQL
instead. **Suggested:** document the seam, or let the memory store's transaction run `beforeCommit` effects
against a `DataSource` the test gives it.

## API friction

11. **Rate limits are per claimed task, per topic.** A topic whose tasks call the partner only sometimes (markers,
    cancels) can't express that (see 5).
12. **`QueueCounts.failed()`** is called `deadLetters` in the endpoint and dashboard; the Java name made the test
    fail to compile on the first try.
13. **The dead-letter view's `error`** is JSON encoded as a string inside the JSON (`"error":"{\"type\": ...}"`),
    so a script has to parse it twice.
14. **POST to the endpoint without a body** gets 415 from Actuator. With a security rule that denies everything
    else, Spring's `/error` dispatch then turns it into a 403, which looked like a CSRF failure. operations.md
    lists `POST .../pause` with no body. Say that every POST needs `Content-Type: application/json` (and `{}`).
15. **Pause is logged at INFO, resume isn't.**
16. **The fan-in identifiers**: since the staging task's own identifier is always part of the group, a part's
    identifier decides what the group is. Parts had to use the order id as identifier and push markers with
    suffixed identifiers. Not obvious from the Javadoc of `Work.junction`.

## Doc gaps

17. **patterns.md's fan-in example can't work.** The merge task is pushed as `invoiceId`, but the group is the
    part's own identifier plus `line-1..3`, none of which has a suspended task in MERGE, so nothing is ever
    released. `EngineTest.junction_releases_the_merge_once_every_part_arrived` shows the working shape: every part
    has its own suspended task, and all are released.
18. **Flyway with Spring Boot.** operations.md shows a plain `Flyway.configure()...migrate()`, but not where to call
    it so it runs before the engine and before the app's own migrations (here a `FlywayMigrationStrategy`). It
    also doesn't say that the app's migrations must grant the runtime role access to the `pgtasks` schema. The
    privilege list for `validate` was right: default privileges for tables and the sequence, plus USAGE on the
    schema and EXECUTE on `create_topic`. A fresh migrate logs `WARN schema "pgtasks" already exists, skipping`,
    because Flyway creates the schema and V001 says `CREATE SCHEMA IF NOT EXISTS`.
19. **Metric names in Prometheus** differ from the table: `pgtasks_completed_tasks_total`,
    `pgtasks_queue_failing_tasks`, `pgtasks_queue_oldest_ready_seconds`. Alerts are written against the Prometheus
    names, so list those.
20. **What a kill -9 costs** (finding 8), and that a rate limit's claims serialize (finding 4).

## What worked well

- Pushing in the request's `@Transactional`: orders and their three tasks commit and roll back together
  (`OrderServicePostgresTest`), and the dedup key kept resent payments to one task.
- `decideConcurrently` with `unavailable`/`throttled`: a total partner outage of 3 minutes and 2 minutes of
  refused connections cost no attempts and no dead letters, and the breakers kept the nodes from hammering the
  partners.
- Retries of completions on deadlocks: 1,712 deadlocks in the app's own stock writes (both runs) were retried without
  losing or duplicating anything.
- Tracing and the request id: every task of an order carried the request's `traceparent` and `requestId`.
- The endpoint: failing groups by error, revive by error type, limit override with a version check (a stale
  version got 409), pause/resume reaching every node at once. Its CSRF handoff through `config.json` worked with
  Spring Security as documented.
- Validate mode with a runtime role without DDL rights, and Flyway with the placeholder: worked first time once the
  grants were in.

## Library commits on this branch

| Commit | What |
|---|---|
| `4d5e8d4` | Release a fan-in group when its parts complete at the same time (finding 1); two `JunctionTck` tests |
| `da630b1` | Fire a recurring task once when the node's clock is behind the database's (finding 3); `RecurringTest` |
| `89fd677` | Hand back a batch whose completion is cut short by stop on a transient error (finding 2); `EngineTest` |

Tests at `89fd677`: pgtasks-postgres 371, pgtasks-memory 135, pgtasks-engine 152, starter 52, pgtasks-test 2, all
passing. The long run used `4d5e8d4`. Fixes 2 and 3 are covered by their tests only, not by another long run.

## Appendix: problems that were the app's own

- **Every basic-auth request ran bcrypt.** `InMemoryUserDetailsManager` upgrades `{noop}` passwords to bcrypt
  after the first login. POST /orders took 65 ms. Fixed with a plain `UserDetailsService` (now ~5 ms).
- **Stock rows deadlocked.** Every stock batch updated the same 50 SKU rows in task order, about 70 deadlocks a
  minute on Postgres. Fixed by locking the batch's rows in SKU order first (`StagedWrites.touchesStock`). The fix
  was deployed by the rolling restart at minute 23, and deadlocks dropped to 0.
- **`anyRequest().denyAll()` blocked Spring's `/error` page**, turning 415s into 403s (see 14).
- **PG15+ `public` has no CREATE for other roles.** The app needs a schema of its own (`orders`).
