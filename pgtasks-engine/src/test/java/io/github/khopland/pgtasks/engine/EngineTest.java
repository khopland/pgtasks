package io.github.khopland.pgtasks.engine;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Junction;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Engine behaviour on the in-memory store: several engines in one JVM, a manual clock, no sleeping past backoffs. */
class EngineTest {

    private static final TopicRef<String> TOPIC = TopicRef.of("topic", String.class);
    private static final TopicRef<String> FIRST = TopicRef.of("first", String.class);
    private static final TopicRef<String> SECOND = TopicRef.of("second", String.class);

    private final MemoryEngines engines = new MemoryEngines();

    @AfterEach
    void tearDown() {
        engines.close();
    }

    private static Topic<String> succeeding(TopicRef<String> topic) {
        return Topic.of(topic, batch -> batch.decide(task -> Decision.success()));
    }

    private TaskView view(TaskTester tester, long id) {
        return tester.find(id).orElseThrow();
    }

    @Test
    void processes_a_pushed_task() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
            seen.add(task.identifier() + ":" + task.payload());
            return Decision.success();
        }))));
        TaskTester tester = engines.tester();

        long id = engine.tasks().independently().push(TOPIC, "person-1", "hello").id();
        tester.awaitIdle();

        assertThat(seen).containsExactly("person-1:hello");
        assertThat(view(tester, id).state()).isEqualTo("succeeded");
    }

    @Test
    @DisplayName("Retries back off on the store's clock and end in a dead letter, without sleeping")
    void retries_then_dead_letter() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
            runs.incrementAndGet();
            return Decision.failed("still broken");
        }))));
        TaskTester tester = engines.tester();

        long id = engine.tasks().independently().push(TOPIC, "x", "payload").id();
        tester.awaitIdle();
        assertThat(view(tester, id).state()).isEqualTo("retrying");
        assertThat(view(tester, id).runAt()).isAfter(engines.clock.instant().plusSeconds(7));

        QueueCounts counts = tester.drain(TOPIC);

        // The standard policy: a week of retries, ramping to every half hour, jumped through on the
        // clock. Jitter shortens backoffs, so a few more runs than the nominal count fit in the week.
        assertThat(runs.get()).isBetween(Retry.standard().nominalAttempts(), Retry.standard().nominalAttempts() * 5 / 4);
        assertThat(counts.failed()).isEqualTo(1);
        TaskView dead = view(tester, id);
        assertThat(dead.state()).isEqualTo("failed");
        assertThat(dead.attempt()).isEqualTo(runs.get());
        assertThat(Duration.between(dead.firstFailedAt(), engines.clock.instant())).as("the last run was in the week's last half hour")
            .isBetween(Duration.ofDays(7).minusMinutes(30), Duration.ofDays(7));
        assertThat(dead.error()).contains("still broken");
    }

    @Test
    void later_runs_again_without_using_an_attempt() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(TOPIC, batch -> batch.decide(task ->
            runs.incrementAndGet() < 3 ? Decision.later(Duration.ofHours(1)) : Decision.success()))));
        TaskTester tester = engines.tester();

        long id = engine.tasks().independently().push(TOPIC, "x", "payload").id();
        tester.drain(TOPIC);

        assertThat(runs).hasValue(3);
        assertThat(view(tester, id).state()).isEqualTo("succeeded");
        assertThat(view(tester, id).attempt()).isEqualTo(1);
    }

    @Test
    void drain_jumps_past_a_delayed_push() throws Exception {
        TaskEngine engine = engines.start("node-a", List.of(succeeding(TOPIC)));
        TaskTester tester = engines.tester();

        long id = engine.tasks().independently().push(TOPIC, Insertion.APPEND, Creation.of("x", "later").withDelay(Duration.ofDays(1))).id();
        tester.awaitIdle();
        assertThat(view(tester, id).state()).isEqualTo("scheduled");

        tester.drain(TOPIC);
        assertThat(view(tester, id).state()).isEqualTo("succeeded");
    }

    @Test
    void a_batch_past_its_deadline_is_cancelled_and_retried() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch interrupted = new CountDownLatch(1);
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
            if (runs.incrementAndGet() == 1) {
                try {
                    Thread.sleep(Duration.ofSeconds(30));
                } catch (InterruptedException e) {
                    interrupted.countDown();
                    throw e;
                }
            }
            return Decision.success();
        })).with(TopicSettings.defaults().withBatchDeadline(Duration.ofMillis(200)))));
        TaskTester tester = engines.tester();

        long id = engine.tasks().independently().push(TOPIC, "x", "payload").id();
        tester.drain(TOPIC);

        assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(view(tester, id).state()).isEqualTo("succeeded");
        assertThat(view(tester, id).attempt()).isEqualTo(2);
    }

    @Test
    void stop_drains_running_batches_and_releases_hung_ones_without_using_an_attempt() throws Exception {
        CountDownLatch started = new CountDownLatch(2);
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
            started.countDown();
            Thread.sleep(task.identifier().equals("hung") ? Duration.ofMinutes(5) : Duration.ofMillis(200));
            return Decision.success();
        })).with(TopicSettings.defaults().withBatchSize(1))));
        TaskTester tester = engines.tester();
        long quick = engine.tasks().independently().push(TOPIC, "quick", "payload").id();
        long hung = engine.tasks().independently().push(TOPIC, "hung", "payload").id();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        engine.stop(Duration.ofMillis(500));

        assertThat(view(tester, quick).state()).isEqualTo("succeeded");
        // Released as due at once, and the housekeeper keeps promoting until the end of stop(), so
        // a promotion between the release and that may already have made it ready.
        assertThat(view(tester, hung).state()).isIn("retrying", "ready");
        assertThat(view(tester, hung).attempt()).isZero();
    }

    @Test
    @DisplayName("Fan-in: parts release the merge task once every part has arrived")
    void junction_releases_the_merge_once_every_part_arrived() throws Exception {
        TopicRef<String> parts = TopicRef.of("parts", String.class);
        TopicRef<String> merge = TopicRef.of("merge", String.class);
        List<String> merged = new CopyOnWriteArrayList<>();
        TaskEngine engine = engines.start("node-a", List.of(
            Topic.of(parts, batch -> batch.decide(task -> {
                // Each part waits in "merge" for its sibling; the group is both parts' identifiers.
                task.push(merge, Insertion.APPEND, Creation.of(task.identifier(), task.payload()).asSuspended());
                task.junction(merge, Junction.Counts.SUSPENDED, Set.of(task.payload()));
                return Decision.success();
            })),
            Topic.of(merge, batch -> batch.decide(task -> {
                merged.add(task.identifier());
                return Decision.success();
            }))
        ));
        TaskTester tester = engines.tester();

        engine.tasks().independently().push(parts, "left", "right");
        tester.awaitIdle();
        assertThat(merged).isEmpty();
        assertThat(tester.counts(merge).suspended()).isEqualTo(1);

        engine.tasks().independently().push(parts, "right", "left");
        tester.awaitIdle();
        assertThat(merged).containsExactlyInAnyOrder("left", "right");
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Ordering.class, names = "SINGULAR.*", mode = EnumSource.Mode.MATCH_ALL)
    @DisplayName("Identifier ordering holds across three engines sharing one store")
    void ordering_across_engines(Ordering ordering) throws Exception {
        Map<String, AtomicInteger> running = new ConcurrentHashMap<>();
        Map<String, List<Long>> succeeded = new ConcurrentHashMap<>();
        List<String> violations = new CopyOnWriteArrayList<>();
        Set<String> nodes = ConcurrentHashMap.newKeySet();
        // At most four identifiers per node at a time (two batches of two), so no node can hold all
        // eight and leave the others nothing for good: finishing a task makes no row newly ready, so
        // nothing would wake them before their next poll.
        for (String node : List.of("node-a", "node-b", "node-c")) {
            engines.start(node, List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
                nodes.add(node);
                if (running.computeIfAbsent(task.identifier(), ignored -> new AtomicInteger()).incrementAndGet() > 1) {
                    violations.add("two tasks for " + task.identifier() + " ran at once");
                }
                try {
                    Thread.sleep(1);
                    if (Integer.parseInt(task.payload()) % 7 == 0 && task.attempt() == 1) {
                        return Decision.failed("first attempt fails");
                    }
                    succeeded.computeIfAbsent(task.identifier(), ignored -> new CopyOnWriteArrayList<>()).add(task.id());
                    return Decision.success();
                } finally {
                    running.get(task.identifier()).decrementAndGet();
                }
            })).with(TopicSettings.defaults().withOrdering(ordering).withConcurrency(2).withBatchSize(2).withPollInterval(Duration.ofMillis(50)))));
        }
        TaskTester tester = engines.tester();
        List<Long> ids = pushAll(120);

        QueueCounts left = tester.drain(TOPIC);

        assertThat(left.total()).isZero();
        assertThat(violations).isEmpty();
        assertThat(nodes).hasSizeGreaterThan(1);
        Map<String, List<Long>> expected = IntStream.range(0, 120).boxed()
            .collect(Collectors.groupingBy(index -> "person-" + index % 8, Collectors.mapping(ids::get, Collectors.toList())));
        assertThat(succeeded).isEqualTo(expected);
    }

    private List<Long> pushAll(int count) {
        TaskEngine engine = engines.create("producer", List.of(), settings -> settings);
        List<Creation<String>> creations = IntStream.range(0, count).mapToObj(index -> Creation.of("person-" + index % 8, String.valueOf(index))).toList();
        return engine.tasks().independently().push(TOPIC, Insertion.APPEND, creations).stream().map(Pushed::id).toList();
    }

    @Test
    @DisplayName("A stale node can't complete a reclaimed task or enqueue its children")
    void stale_node_is_fenced_out() throws Exception {
        CountDownLatch stalled = new CountDownLatch(1), resume = new CountDownLatch(1);
        List<Topic<?>> stuck = List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
            stalled.countDown();
            resume.await();
            task.push(FIRST, task.identifier(), "from node-a");
            return Decision.success();
        })).with(TopicSettings.defaults().withConcurrency(1)), succeeding(FIRST));
        // Node C runs the cluster-wide housekeeping. (HousekeepingTest covers a leader that hangs.)
        TaskEngine nodeC = engines.start("node-c", List.of());
        EngineFixture.await("node C to lead", Duration.ofSeconds(5), nodeC::leadsHousekeeping);
        // Node A sends no heartbeat after starting, as if it were paused.
        TaskEngine nodeA = engines.start("node-a", stuck, settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(2)));
        long id = nodeA.tasks().independently().push(TOPIC, "x", "payload").id();
        assertThat(stalled.await(5, TimeUnit.SECONDS)).isTrue();

        engines.start("node-b", List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
            task.push(FIRST, task.identifier(), "from node-b");
            return Decision.success();
        })), succeeding(FIRST)));
        TaskTester tester = engines.tester();
        engines.clock.advance(Duration.ofMinutes(11));
        // Expired: it waits out the expiry backoff before another node may run it.
        EngineFixture.await("the task to expire", Duration.ofSeconds(10), () -> "retrying".equals(tester.find(id).orElseThrow().state()));
        engines.clock.advance(Duration.ofMinutes(1));
        EngineFixture.await("node B to finish the reclaimed task", Duration.ofSeconds(10),
            () -> "succeeded".equals(tester.find(id).orElseThrow().state()));

        resume.countDown();
        nodeA.stop(Duration.ofSeconds(5)); // waits for A's batch to finish its rejected completion
        tester.awaitIdle();

        assertThat(tester.find(id).orElseThrow().attempt()).isEqualTo(2);
        assertThat(tester.pushed(FIRST)).extracting(Creation::payload).containsExactly("from node-b");
    }

    /** Stages work per task and records it after the commit, only for tasks that continue. */
    private static final class Publisher implements BatchResource<List<Task<?>>> {

        final List<String> published = new CopyOnWriteArrayList<>();

        @Override
        public List<Task<?>> open(TopicRef<?> topic, List<Task<?>> tasks) {
            return new CopyOnWriteArrayList<>();
        }

        @Override
        public void afterCommit(List<Task<?>> staged, Decisions finalDecisions) {
            staged.stream().filter(task -> finalDecisions.decision(task).continues()).forEach(task -> published.add(task.identifier()));
        }
    }

    /** Vetoes blocked identifiers during review. */
    private static final class Veto implements BatchResource<Void> {

        @Override
        public Void open(TopicRef<?> topic, List<Task<?>> tasks) {
            return null;
        }

        @Override
        public Decisions review(Void resource, Decisions decisions) {
            Decisions reviewed = decisions;
            for (Task<?> task : decisions.tasks()) {
                if (task.identifier().startsWith("blocked")) {
                    reviewed = reviewed.revise(task, Decision.filtered("blocked"));
                }
            }
            return reviewed;
        }
    }

    @Test
    @DisplayName("A parent vetoed by another resource gets no child and no publication, in either resource order")
    void vetoed_parents_get_nothing() throws Exception {
        for (boolean publisherFirst : List.of(true, false)) {
            Publisher publisher = new Publisher();
            Veto veto = new Veto();
            TopicRef<String> topic = TopicRef.of("vetoed-" + publisherFirst, String.class);
            TopicRef<String> child = TopicRef.of("child-" + publisherFirst, String.class);
            engines.createTopics(topic.name(), child.name());
            TaskEngine engine = engines.create("node-" + publisherFirst, List.of(
                new Topic<>(topic, batch -> {
                    List<Task<?>> staged = batch.resource(publisher);
                    return batch.decide(task -> {
                        staged.add(task.task());
                        task.push(child, task.identifier(), "child");
                        return Decision.success();
                    });
                }, PayloadCodec.string(), TopicSettings.defaults(), publisherFirst ? List.of(publisher, veto) : List.of(veto, publisher)),
                succeeding(child)
            ), settings -> settings);
            engine.tasks().independently().push(topic, Insertion.APPEND, List.of(
                Creation.of("anna", "x"), Creation.of("blocked-bo", "x"), Creation.of("carl", "x")));
            TaskTester tester = TaskTester.of(engine);
            engine.start();
            tester.awaitIdle();

            assertThat(publisher.published).containsExactlyInAnyOrder("anna", "carl");
            assertThat(tester.pushed(child)).extracting(Creation::identifier).containsExactlyInAnyOrder("anna", "carl");
            engine.stop(Duration.ofSeconds(5));
        }
    }

    @Test
    void a_mixed_batch_with_two_downstream_topics_completes() throws Exception {
        engines.createTopics(TOPIC.name(), FIRST.name(), SECOND.name());
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        TaskEngine engine = engines.create("node-a", List.of(
            Topic.of(TOPIC, batch -> {
                batchSizes.add(batch.tasks().size());
                return batch.decide(task -> switch (task.payload()) {
                    case "both" -> {
                        task.push(FIRST, task.identifier(), "x");
                        task.push(SECOND, task.identifier(), "x");
                        yield Decision.success();
                    }
                    case "first" -> {
                        task.push(FIRST, task.identifier(), "x");
                        yield Decision.success();
                    }
                    case "filtered" -> {
                        task.push(FIRST, task.identifier(), "never");
                        task.push(SECOND, task.identifier(), "never");
                        yield Decision.filtered("irrelevant");
                    }
                    case "failed" -> {
                        task.push(SECOND, task.identifier(), "never");
                        yield Decision.failed("boom");
                    }
                    case "throws" -> throw new IllegalStateException("bug");
                    default -> Decision.suspended();
                });
            }),
            succeeding(FIRST),
            succeeding(SECOND)
        ), settings -> settings);
        List<String> kinds = List.of("both", "filtered", "first", "failed", "suspended", "both", "throws");
        engine.tasks().independently().push(TOPIC, Insertion.APPEND, IntStream.range(0, kinds.size())
            .mapToObj(index -> Creation.of(kinds.get(index) + "-" + index, kinds.get(index))).toList());
        TaskTester tester = TaskTester.of(engine);
        engine.start();
        tester.awaitIdle();

        assertThat(batchSizes.getFirst()).isEqualTo(kinds.size());
        assertThat(tester.pushed(FIRST)).extracting(Creation::identifier).containsExactly("both-0", "first-2", "both-5");
        assertThat(tester.pushed(SECOND)).extracting(Creation::identifier).containsExactly("both-0", "both-5");
        QueueCounts counts = tester.counts(TOPIC);
        assertThat(counts.retrying()).isEqualTo(2);
        assertThat(counts.suspended()).isEqualTo(1);
    }

    @Test
    @DisplayName("Pushes in a transaction are held back until it commits, and dropped if it rolls back")
    void held_back_pushes() throws Exception {
        LocalTransactions transactions = new LocalTransactions();
        TaskEngine engine = engines.start("node-a", List.of(succeeding(TOPIC)), settings -> settings.withTransactions(transactions));
        TaskTester tester = engines.tester();

        assertThatThrownBy(() -> engine.tasks().push(TOPIC, "x", "payload")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> transactions.inTransaction(() -> {
            assertThat(engine.tasks().push(TOPIC, "dropped", "payload").status()).isEqualTo(Pushed.Status.PENDING);
            throw new IllegalStateException("rolled back");
        })).hasMessage("rolled back");
        transactions.inTransaction(() -> {
            engine.tasks().push(TOPIC, "kept", "payload");
            assertThat(tester.counts(TOPIC).total()).as("nothing before the commit").isZero();
            return null;
        });
        tester.awaitIdle();

        assertThat(tester.pushed(TOPIC)).extracting(Creation::identifier).containsExactly("kept");
        assertThatThrownBy(() -> transactions.inTransaction(() -> engine.tasks().push(TopicRef.of("missing", String.class), "x", "y")))
            .as("unknown topics fail at push time, not after the commit")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("A claim whose reply never arrived is released by the node's orphan sweep")
    void orphaned_claims_are_released() throws Exception {
        TaskEngine engine = engines.start("node-a", List.of(succeeding(TOPIC)), settings -> settings.withHeartbeat(Duration.ofMillis(50), Duration.ofHours(1)));
        engines.createTopics("other");
        TaskTester tester = engines.tester();
        long id;
        // Claimed for this node in the store, but the engine never saw the claim.
        try (var transaction = engines.store.begin()) {
            int topicId = engines.store.findTopic(transaction, TOPIC.name()).orElseThrow();
            id = engines.store.push(transaction, topicId, Insertion.APPEND, List.of(new TaskStore.NewTask("lost", null, null, null, null, false, 0, null, null, null, Map.of()))).getFirst().id();
            engines.store.claim(transaction, new TaskStore.ClaimRequest(topicId, engine.nodeId(), Ordering.FIFO, 1, null));
            transaction.commit();
        }
        assertThat(view(tester, id).state()).isEqualTo("active");

        engines.clock.advance(Duration.ofSeconds(3));
        EngineFixture.await("the orphan to be released and run", Duration.ofSeconds(5), () -> "succeeded".equals(view(tester, id).state()));
        assertThat(view(tester, id).attempt()).as("the lost claim didn't count").isEqualTo(1);
    }

    @Test
    void the_in_memory_store_cannot_join_jdbc_transactions_or_lend_a_connection() throws Exception {
        TaskEngine engine = engines.start("node-a", List.of(succeeding(TOPIC)));
        Connection connection = (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (proxy, method, arguments) -> null);
        assertThatThrownBy(() -> engine.tasks().on(connection))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("independently()");

        try (var transaction = engines.store.begin()) {
            assertThatThrownBy(() -> transaction.connection().prepareStatement("SELECT 1"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("needs the Postgres store");
        }
    }
}
