package io.github.khopland.pgtasks.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static io.github.khopland.pgtasks.engine.EngineFixture.await;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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

import javax.sql.DataSource;

import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.slice.AssessmentHandler;
import io.github.khopland.pgtasks.slice.AssessmentLedger;
import io.github.khopland.pgtasks.slice.FeedPublisher;
import io.github.khopland.pgtasks.slice.IncomeFeed;
import io.github.khopland.pgtasks.slice.NoticeHandler;
import io.github.khopland.pgtasks.slice.PartsRegister;
import io.github.khopland.pgtasks.slice.Slice;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** The milestone 1 acceptance tests from plan section 15, on real Postgres with several workers. */
class MilestoneOneTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    /** Retries that wait for the test: makeDue() releases them. */
    private static final Retry PARKED = Retry.exponential(Duration.ofHours(1), Duration.ofHours(1)).attempts(5).withJitter(0);
    private static final Retry FAST = Retry.exponential(Duration.ofMillis(50), Duration.ofMillis(400)).attempts(5).withJitter(0.5);

    private EngineFixture fixture;
    private PostgresDatabase database;

    @BeforeEach
    void setUp(PostgresDatabase database) {
        this.database = database;
        fixture = new EngineFixture(database);
        Slice.createDomainTables(database);
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    private static Topic<String> archive() {
        return Topic.of(Slice.ARCHIVE, batch -> batch.decide(task -> Decision.success()));
    }

    private List<Topic<?>> slice(AssessmentHandler assessments, FeedPublisher publisher, Retry retry, BatchResource<?>... noticeResources) {
        List<BatchResource<?>> resources = new ArrayList<>();
        resources.add(publisher);
        resources.addAll(List.of(noticeResources));
        return List.of(
            new Topic<>(Slice.ASSESSMENT, assessments, Slice.REQUESTS, TopicSettings.defaults().withRetry(retry), List.of()),
            new Topic<>(Slice.NOTIFY, new NoticeHandler(publisher), Slice.NOTICES, TopicSettings.defaults().withRetry(retry), resources),
            archive()
        );
    }

    private long tasksIn(TopicRef<?> topic) {
        return fixture.count("SELECT count(*) FROM tasks.task_view WHERE topic = ?", topic.name());
    }

    @Test
    @DisplayName("1. A feed rollback leaves neither a task nor an advanced pointer")
    void feed_rollback_leaves_neither_task_nor_pointer() throws Exception {
        PartsRegister register = new PartsRegister().register("anna", 30).register("bjorn", 40);
        AssessmentLedger ledger = new AssessmentLedger(database.dataSource());
        TaskEngine engine = fixture.start("node-a", slice(new AssessmentHandler(register, ledger), new FeedPublisher(), FAST));
        IncomeFeed feed = new IncomeFeed(engine.tasks());
        database.execute("INSERT INTO incoming (person, income) VALUES ('anna', 500000), ('bjorn', 600000)");
        DataSource pool = database.dataSource();

        try (Connection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            assertThat(feed.readPage(connection, 10)).isEqualTo(2);
            connection.rollback(); // say the feed failed after reading the page
        }
        Thread.sleep(300);
        assertThat(fixture.count("SELECT count(*) FROM tasks.task")).isZero();
        assertThat(fixture.count("SELECT position FROM feed_pointer")).isZero();
        assertThat(ledger.calls).hasValue(0);

        try (Connection connection = pool.getConnection()) {
            connection.setAutoCommit(false);
            feed.readPage(connection, 10);
            connection.commit();
        }
        assertThat(fixture.count("SELECT position FROM feed_pointer")).isEqualTo(2);
        await("both publications", Duration.ofSeconds(10), () -> fixture.count("SELECT count(*) FROM published_feed") == 2);
        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
        assertThat(tasksIn(Slice.ARCHIVE)).isEqualTo(2);
    }

    @Test
    @DisplayName("2. A completion rollback leaves neither a JDBC publication nor a child task")
    void completion_rollback_leaves_neither_publication_nor_child() throws Exception {
        FeedPublisher publisher = new FeedPublisher();
        publisher.failures.set(1);
        TaskEngine engine = fixture.start("node-a", slice(new AssessmentHandler(new PartsRegister(), new AssessmentLedger(database.dataSource())), publisher, PARKED));

        engine.tasks().independently().push(Slice.NOTIFY, Insertion.APPEND, List.of(
            Creation.of("anna", new Slice.Notice("anna", 150000)),
            Creation.of("bjorn", new Slice.Notice("bjorn", 240000))
        ));

        await("the failed first attempt", Duration.ofSeconds(10), () -> fixture.count("SELECT count(*) FROM tasks.task_view WHERE topic = 'notify' AND state = 'retrying'") == 2);
        assertThat(fixture.count("SELECT count(*) FROM published_feed")).as("publications").isZero();
        assertThat(tasksIn(Slice.ARCHIVE)).as("child tasks").isZero();
        assertThat(database.rows("SELECT DISTINCT result, error->>'message' FROM tasks.attempt"))
            .containsExactly(List.of("failed", "Simulated failure while publishing"));

        fixture.makeDue();
        fixture.awaitQueueEmpty(Duration.ofSeconds(10));
        assertThat(fixture.count("SELECT count(*) FROM published_feed")).isEqualTo(2);
        assertThat(tasksIn(Slice.ARCHIVE)).isEqualTo(2);
        assertThat(database.rows("SELECT state, attempt FROM tasks.task_view WHERE topic = 'notify'")).containsOnly(List.of("succeeded", 2));
    }

    @Test
    @DisplayName("3. A stale worker can't complete a reclaimed task or enqueue its children")
    void stale_worker_cannot_complete_or_enqueue() throws Exception {
        PartsRegister register = new PartsRegister().register("anna", 30);
        AssessmentLedger ledger = new AssessmentLedger(database.dataSource());
        CountDownLatch stalled = new CountDownLatch(1), resume = new CountDownLatch(1);
        AssessmentHandler stuck = new AssessmentHandler(register, ledger).afterChange(task -> {
            stalled.countDown();
            try {
                resume.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        // Node A runs one assessment batch at a time and never sends another heartbeat after
        // starting, as if it were paused.
        TaskEngine nodeA = fixture.start("node-a", List.of(
            new Topic<>(Slice.ASSESSMENT, stuck, Slice.REQUESTS, TopicSettings.defaults().withConcurrency(1).withRetry(FAST), List.of())
        ), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(2)));
        nodeA.tasks().independently().push(Slice.ASSESSMENT, "anna", new Slice.AssessmentRequest("anna", 500000));
        assertThat(stalled.await(10, TimeUnit.SECONDS)).isTrue();

        // As if node A had been paused for an hour: its heartbeat is old and its housekeeping lease ran out.
        database.execute("UPDATE tasks.node SET heartbeat_at = now() - interval '1 hour' WHERE id = " + nodeA.nodeId());
        database.execute("UPDATE tasks.lease SET expires_at = now() - interval '1 hour' WHERE node_id = " + nodeA.nodeId());
        fixture.start("node-b", slice(new AssessmentHandler(register, ledger), new FeedPublisher(), FAST),
            settings -> settings.withHeartbeat(Duration.ofMillis(100), Duration.ofMillis(500)));
        await("node B to finish the reclaimed task", Duration.ofSeconds(10), () -> fixture.count(
            "SELECT count(*) FROM tasks.task_view WHERE topic = 'assessment' AND state = 'succeeded' AND attempt = 2") == 1);

        resume.countDown();
        nodeA.stop(Duration.ofSeconds(10)); // waits for A's batch to finish its (rejected) completion
        fixture.awaitQueueEmpty(Duration.ofSeconds(10));

        assertThat(tasksIn(Slice.NOTIFY)).as("notices: only node B's").isEqualTo(1);
        assertThat(database.rows("SELECT result, node_id FROM tasks.attempt WHERE result = 'expired'")).containsExactly(List.of("expired", nodeA.nodeId()));
        assertThat(fixture.count("SELECT count(*) FROM published_feed")).isEqualTo(1);
        assertThat(ledger.calls).as("both nodes made the external call").hasValue(2);
        assertThat(fixture.count("SELECT count(*) FROM assessment")).as("deduplicated by the stable idempotency key").isEqualTo(1);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Ordering.class, names = "SINGULAR.*", mode = EnumSource.Mode.MATCH_ALL)
    @DisplayName("4. The topic keeps its identifier ordering across several workers")
    void identifier_ordering_holds_across_workers(Ordering ordering) throws Exception {
        TopicRef<String> topic = TopicRef.of("ordered", String.class);
        Map<String, AtomicInteger> running = new ConcurrentHashMap<>();
        Map<String, List<Long>> succeeded = new ConcurrentHashMap<>();
        List<String> violations = new CopyOnWriteArrayList<>();
        Set<String> nodes = ConcurrentHashMap.newKeySet();
        for (String node : List.of("node-a", "node-b", "node-c")) {
            fixture.start(node, List.of(Topic.of(topic, batch -> batch.decide(task -> {
                nodes.add(node);
                if (running.computeIfAbsent(task.identifier(), ignored -> new AtomicInteger()).incrementAndGet() > 1) {
                    violations.add("two tasks for " + task.identifier() + " ran at once");
                }
                try {
                    Thread.sleep(2);
                    // Every ninth task fails once and backs off; later tasks for its identifier must wait.
                    if (Integer.parseInt(task.payload()) % 9 == 0 && task.attempt() == 1) {
                        return Decision.failed("first attempt fails");
                    }
                    succeeded.computeIfAbsent(task.identifier(), ignored -> new CopyOnWriteArrayList<>()).add(task.id());
                    return Decision.success();
                } finally {
                    running.get(task.identifier()).decrementAndGet();
                }
            })).with(TopicSettings.defaults().withOrdering(ordering).withConcurrency(3).withBatchSize(4).withRetry(FAST))));
        }
        // More identifiers than one node can hold at once (3 batches of 4), so every node gets work.
        int identifiers = 30;
        List<Creation<String>> creations = IntStream.range(0, 150).mapToObj(index -> Creation.of("person-" + index % identifiers, String.valueOf(index))).toList();
        List<Long> ids = fixture.engines().getFirst().tasks().independently().push(topic, Insertion.APPEND, creations).stream().map(pushed -> pushed.id()).toList();

        fixture.awaitQueueEmpty(Duration.ofSeconds(60));

        assertThat(violations).isEmpty();
        assertThat(nodes).as("work was spread over several workers").hasSizeGreaterThan(1);
        Map<String, List<Long>> expected = IntStream.range(0, 150).boxed()
            .collect(Collectors.groupingBy(index -> "person-" + index % identifiers, Collectors.mapping(ids::get, Collectors.toList())));
        assertThat(succeeded).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Ordering.class, names = {"SINGULAR_BY_IDENTIFIER", "SINGULAR_BY_IDENTIFIER_ON_FAILURE"})
    @DisplayName("4. A dead-lettered task lets later ones run under SINGULAR, and holds them under ON_FAILURE")
    void dead_letter_follows_the_ordering_table(Ordering ordering) throws Exception {
        TopicRef<String> topic = TopicRef.of("ordered", String.class);
        List<String> events = new CopyOnWriteArrayList<>();
        for (String node : List.of("node-a", "node-b")) {
            fixture.start(node, List.of(Topic.of(topic, batch -> batch.decide(task -> {
                events.add(task.payload() + "#" + task.attempt());
                return task.payload().equals("poison") ? Decision.failed("always fails") : Decision.success();
            })).with(TopicSettings.defaults().withOrdering(ordering).withRetry(Retry.exponential(Duration.ofMillis(50), Duration.ofMillis(50)).attempts(3).withJitter(0)))));
        }
        fixture.engines().getFirst().tasks().independently().push(topic, Insertion.APPEND, List.of(
            Creation.of("x", "poison"), Creation.of("x", "later-1"), Creation.of("x", "later-2"), Creation.of("y", "other")
        ));

        await("the poison task to be dead-lettered", Duration.ofSeconds(10), () -> fixture.count("SELECT count(*) FROM tasks.task_view WHERE state = 'failed'") == 1);
        if (ordering == Ordering.SINGULAR_BY_IDENTIFIER) {
            await("the later tasks", Duration.ofSeconds(10), () -> fixture.count("SELECT count(*) FROM tasks.queue") == 1);
            assertThat(events.subList(0, events.indexOf("later-1#1")))
                .as("everything for x before the later tasks ran").contains("poison#1", "poison#2", "poison#3");
            assertThat(events).containsSubsequence("poison#3", "later-1#1", "later-2#1");
        } else {
            await("the other identifier", Duration.ofSeconds(10), () -> events.contains("other#1"));
            Thread.sleep(500);
            assertThat(events).doesNotContain("later-1#1", "later-2#1");
            assertThat(database.rows("SELECT payload, state FROM tasks.task_view WHERE identifier = 'x' ORDER BY id"))
                .containsExactly(List.of("poison", "failed"), List.of("later-1", "ready"), List.of("later-2", "ready"));
        }
    }

    @Test
    @DisplayName("5. The hard rate ceiling holds across several workers and pods")
    void rate_ceiling_holds_across_pods() throws Exception {
        TopicRef<String> topic = TopicRef.of("limited", String.class);
        double rate = 20;
        int burst = 5;
        List<Instant> claims = new CopyOnWriteArrayList<>();
        Set<String> nodes = ConcurrentHashMap.newKeySet();
        for (String node : List.of("pod-a", "pod-b", "pod-c")) {
            fixture.start(node, List.of(Topic.of(topic, batch -> {
                nodes.add(node);
                batch.tasks().forEach(task -> claims.add(task.claimedAt()));
                return Decisions.all(batch.tasks(), Decision.success());
            }).with(TopicSettings.defaults().withConcurrency(3).withBatchSize(10).withRateLimit(RateLimit.perSecond("downstream", rate, burst)))));
        }
        int total = 70;
        fixture.engines().getFirst().tasks().independently().push(topic, Insertion.APPEND,
            IntStream.range(0, total).mapToObj(index -> Creation.of("t" + index, "x")).toList());

        fixture.awaitQueueEmpty(Duration.ofSeconds(30));

        List<Instant> sorted = new ArrayList<>(claims);
        Collections.sort(sorted);
        assertThat(sorted).hasSize(total);
        // Over any window from claim i to claim j: j - i + 1 <= burst + rate * (t_j - t_i), with 50 ms for clock reads.
        for (int i = 0; i < sorted.size(); i++) {
            for (int j = i; j < sorted.size(); j++) {
                double seconds = Duration.between(sorted.get(i), sorted.get(j)).toNanos() / 1e9;
                assertThat((double) (j - i + 1))
                    .as("claims %d..%d within %.3f s", i, j, seconds)
                    .isLessThanOrEqualTo(burst + rate * (seconds + 0.05));
            }
        }
        double elapsed = Duration.between(sorted.getFirst(), sorted.getLast()).toNanos() / 1e9;
        assertThat(elapsed).as("70 claims at 20/s with a burst of 5 need at least 3.25 s").isGreaterThan((total - burst) / rate - 0.05);
        assertThat(nodes).as("several pods took part").hasSizeGreaterThan(1);
    }

    /** Vetoes notices for blocked persons during review, without writing anything. */
    private static final class Compliance implements BatchResource<Void> {

        private final boolean asFailure;

        Compliance(boolean asFailure) {
            this.asFailure = asFailure;
        }

        @Override
        public Void open(TopicRef<?> topic, List<Task<?>> tasks) {
            return null;
        }

        @Override
        public Decisions review(Void resource, Decisions decisions) {
            Decisions reviewed = decisions;
            for (Task<?> task : decisions.tasks()) {
                if (((Slice.Notice) task.payload()).person().startsWith("blocked")) {
                    reviewed = reviewed.revise(task, asFailure ? Decision.failed("blocked by compliance") : Decision.filtered("blocked by compliance"));
                }
            }
            return reviewed;
        }

        @Override
        public String toString() {
            return "Compliance";
        }
    }

    @ParameterizedTest(name = "veto as failure: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("6. No child or publication for a parent another resource stopped, in either resource order")
    void a_vetoed_parent_gets_no_child_or_publication(boolean asFailure) throws Exception {
        for (boolean publisherFirst : List.of(true, false)) {
            database.execute("TRUNCATE published_feed");
            FeedPublisher publisher = new FeedPublisher();
            Compliance compliance = new Compliance(asFailure);
            List<BatchResource<?>> resources = publisherFirst ? List.of(publisher, compliance) : List.of(compliance, publisher);
            String suffix = publisherFirst ? "-a" : "-b";
            TopicRef<Slice.Notice> notify = TopicRef.of("notify" + suffix, Slice.Notice.class);
            TopicRef<String> archive = TopicRef.of("archive" + suffix, String.class);
            fixture.createTopics(notify.name(), archive.name());
            TaskEngine engine = fixture.create("node" + suffix, List.of(
                new Topic<>(notify, batch -> {
                    FeedPublisher.Staging staging = batch.resource(publisher);
                    return batch.decide(task -> {
                        staging.publish(task.task(), task.payload().person(), "tax " + task.payload().tax());
                        task.push(archive, task.identifier(), "notice " + task.id());
                        return Decision.success();
                    });
                }, Slice.NOTICES, TopicSettings.defaults().withRetry(PARKED), resources),
                Topic.of(archive, batch -> batch.decide(task -> Decision.success()))
            ), settings -> settings);
            engine.tasks().independently().push(notify, Insertion.APPEND, List.of(
                Creation.of("anna", new Slice.Notice("anna", 1)),
                Creation.of("blocked-bo", new Slice.Notice("blocked-bo", 2)),
                Creation.of("carl", new Slice.Notice("carl", 3)),
                Creation.of("blocked-dag", new Slice.Notice("blocked-dag", 4))
            ));
            engine.start();

            String vetoed = asFailure ? "retrying" : "filtered";
            await("one batch to complete", Duration.ofSeconds(10), () -> fixture.count(
                "SELECT count(*) FROM tasks.task_view WHERE topic = ? AND state IN ('succeeded', ?)", notify.name(), vetoed) == 4);
            await("the archive children", Duration.ofSeconds(10), () -> fixture.count(
                "SELECT count(*) FROM tasks.task_view WHERE topic = ? AND state = 'succeeded'", archive.name()) == 2);

            assertThat(database.rows("SELECT person FROM published_feed ORDER BY person")).containsExactly(List.of("anna"), List.of("carl"));
            assertThat(database.rows("SELECT identifier FROM tasks.task_view WHERE topic = ? ORDER BY identifier", archive.name()))
                .containsExactly(List.of("anna"), List.of("carl"));
            assertThat(database.rows("SELECT identifier, state FROM tasks.task_view WHERE topic = ? ORDER BY identifier", notify.name()))
                .containsExactly(List.of("anna", "succeeded"), List.of("blocked-bo", vetoed), List.of("blocked-dag", vetoed), List.of("carl", "succeeded"));
            engine.stop(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("7. A batch with mixed outcomes and several downstream topics completes correctly")
    void mixed_outcomes_with_several_downstream_topics() throws Exception {
        TopicRef<String> mixed = TopicRef.of("mixed", String.class);
        TopicRef<String> first = TopicRef.of("first-target", String.class);
        TopicRef<String> second = TopicRef.of("second-target", String.class);
        fixture.createTopics(mixed.name(), first.name(), second.name());
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        TaskEngine engine = fixture.create("node-a", List.of(
            Topic.of(mixed, batch -> {
                batchSizes.add(batch.tasks().size());
                return batch.decide(task -> switch (task.payload()) {
                    case "success-both" -> {
                        task.push(first, task.identifier(), "from " + task.id());
                        task.push(second, task.identifier(), "from " + task.id());
                        yield Decision.success();
                    }
                    case "success-first" -> {
                        task.push(first, task.identifier(), "from " + task.id());
                        yield Decision.success();
                    }
                    case "success-none" -> Decision.success();
                    case "filtered" -> {
                        task.push(first, task.identifier(), "never");
                        task.push(second, task.identifier(), "never");
                        yield Decision.filtered("irrelevant");
                    }
                    case "failed" -> {
                        task.push(second, task.identifier(), "never");
                        yield Decision.failed("boom");
                    }
                    case "suspended" -> {
                        task.push(first, task.identifier(), "never");
                        yield Decision.suspended();
                    }
                    case "later" -> Decision.later(Duration.ofHours(1));
                    case "throttled" -> {
                        task.push(first, task.identifier(), "never");
                        yield Decision.throttled(Duration.ofHours(1));
                    }
                    case "throws" -> throw new IllegalStateException("handler bug for one task");
                    default -> throw new IllegalArgumentException(task.payload());
                });
            }).with(TopicSettings.defaults().withRetry(PARKED)),
            Topic.of(first, batch -> batch.decide(task -> Decision.success())),
            Topic.of(second, batch -> batch.decide(task -> Decision.success()))
        ), settings -> settings);
        List<String> kinds = List.of("success-both", "filtered", "success-first", "failed", "suspended", "success-both", "later", "throttled", "success-none", "throws");
        engine.tasks().independently().push(mixed, Insertion.APPEND, IntStream.range(0, kinds.size())
            .mapToObj(index -> Creation.of(kinds.get(index) + "-" + index, kinds.get(index))).toList());
        engine.start();

        await("the children to finish", Duration.ofSeconds(10), () -> fixture.count(
            "SELECT count(*) FROM tasks.task_view WHERE topic IN ('first-target', 'second-target') AND state = 'succeeded'") == 5);

        assertThat(batchSizes.getFirst()).as("everything came in one batch").isEqualTo(kinds.size());
        assertThat(database.rows("SELECT identifier FROM tasks.task_view WHERE topic = 'first-target' ORDER BY id"))
            .containsExactly(List.of("success-both-0"), List.of("success-first-2"), List.of("success-both-5"));
        assertThat(database.rows("SELECT identifier FROM tasks.task_view WHERE topic = 'second-target' ORDER BY id"))
            .containsExactly(List.of("success-both-0"), List.of("success-both-5"));
        assertThat(database.rows("SELECT payload, state, attempt FROM tasks.task_view WHERE topic = 'mixed' ORDER BY id")).containsExactly(
            List.of("success-both", "succeeded", 1),
            List.of("filtered", "filtered", 1),
            List.of("success-first", "succeeded", 1),
            List.of("failed", "retrying", 1),
            List.of("suspended", "suspended", 0),
            List.of("success-both", "succeeded", 1),
            List.of("later", "scheduled", 0),
            List.of("throttled", "retrying", 0),
            List.of("success-none", "succeeded", 1),
            List.of("throws", "retrying", 1)
        );
        assertThat(database.rows("SELECT t.parent_id = p.id, t.root_id = p.id FROM tasks.task_view t JOIN tasks.task_view p ON p.id = t.parent_id WHERE t.topic = 'first-target'"))
            .containsOnly(List.of(true, true));
    }
}
