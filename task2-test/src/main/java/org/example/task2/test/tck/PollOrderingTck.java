package org.example.task2.test.tck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.example.task2.test.tck.StoreFixture.ids;
import static org.example.task2.test.tck.StoreFixture.prioritized;
import static org.example.task2.test.tck.StoreFixture.task;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.example.task2.Insertion;
import org.example.task2.Ordering;
import org.example.task2.spi.StoreTransaction;
import org.example.task2.spi.TaskStore;
import org.example.task2.spi.TaskStore.ClaimedTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** TCK: the per-identifier ordering contract from plan section 10, cell by cell. */
public abstract class PollOrderingTck extends StoreTck {

    private int topic, node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
        return fixture;
    }

    /** Puts one task for identifier "x" into the given state and returns its id. */
    private long earlier(StoreFixture fixture, String state) throws Exception {
        long id = fixture.push(topic, "x");
        switch (state) {
            case "ready" -> { }
            case "active" -> fixture.claim(topic, node);
            case "backing off" -> fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.retrying(Duration.ofHours(1)));
            case "throttled" -> fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.throttled(Duration.ofHours(1)));
            case "expired" -> {
                int crashed = fixture.node("crashed");
                fixture.claim(topic, crashed);
                fixture.advance(Duration.ofHours(1));
                fixture.heartbeat(node);
                assertThat(fixture.requeueExpired(node, Duration.ofMinutes(10))).containsExactly(topic);
            }
            case "suspended" -> fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.suspension());
            case "delayed" -> fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.later(Duration.ofHours(1)));
            case "dead-lettered" -> fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.deadLetter());
            case "finished" -> fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
            default -> throw new IllegalArgumentException(state);
        }
        return id;
    }

    static Stream<Arguments> table() {
        // earlier task is..., SINGULAR, ON_FAILURE, UNTIL_SUCCESS (true = the later task runs)
        return Stream.of(
            Arguments.of("ready", false, false, false),
            Arguments.of("active", false, false, false),
            Arguments.of("backing off", false, false, false),
            Arguments.of("throttled", false, false, false),
            Arguments.of("expired", false, false, false),
            Arguments.of("suspended", true, true, false),
            Arguments.of("delayed", true, true, false),
            Arguments.of("dead-lettered", true, false, false),
            Arguments.of("finished", true, true, true)
        ).flatMap(row -> {
            Object[] values = row.get();
            return Stream.of(
                Arguments.of(values[0], Ordering.FIFO, true),
                Arguments.of(values[0], Ordering.SINGULAR_BY_IDENTIFIER, values[1]),
                Arguments.of(values[0], Ordering.SINGULAR_BY_IDENTIFIER_ON_FAILURE, values[2]),
                Arguments.of(values[0], Ordering.SINGULAR_BY_IDENTIFIER_UNTIL_SUCCESS, values[3])
            );
        });
    }

    @ParameterizedTest(name = "earlier task {0}, {1}: later task runs = {2}")
    @MethodSource("table")
    void later_task_waits_or_runs_as_the_table_says(String state, Ordering ordering, boolean runs) throws Exception {
        StoreFixture fixture = setUp();
        long earlier = earlier(fixture, state);
        long later = fixture.push(topic, "x");

        List<Long> claimed = ids(fixture.claim(topic, node, ordering, 10));

        assertThat(claimed.contains(later)).as("later task claimed").isEqualTo(runs);
        if (state.equals("ready") || state.equals("expired")) {
            assertThat(claimed).as("the earlier task itself is ready").contains(earlier);
        }
    }

    @ParameterizedTest
    @EnumSource(value = Ordering.class, names = "SINGULAR.*", mode = EnumSource.Mode.MATCH_ALL)
    void only_tasks_in_the_same_topic_count(Ordering ordering) throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.topic("other");
        fixture.push(other, "x");
        fixture.claim(other, node);
        fixture.push(other, "x");
        long mine = fixture.push(topic, "x");

        assertThat(ids(fixture.claim(topic, node, ordering, 10))).containsExactly(mine);
    }

    @ParameterizedTest
    @EnumSource(value = Ordering.class, names = "SINGULAR.*", mode = EnumSource.Mode.MATCH_ALL)
    void later_failed_tasks_do_not_hold_back_earlier_ones(Ordering ordering) throws SQLException {
        StoreFixture fixture = setUp();
        long first = fixture.push(topic, Insertion.APPEND, prioritized("x", 0)).getFirst().id();
        fixture.push(topic, Insertion.APPEND, prioritized("x", 5));
        // FIFO by priority claims the later task first; it dead-letters.
        ClaimedTask later = fixture.claim(topic, node, Ordering.FIFO, 1).getFirst();
        fixture.complete(topic, node, later, StoreFixture.deadLetter());

        assertThat(ids(fixture.claim(topic, node, ordering, 10))).containsExactly(first);
    }

    @ParameterizedTest
    @EnumSource(value = Ordering.class, names = "SINGULAR.*", mode = EnumSource.Mode.MATCH_ALL)
    void an_active_task_blocks_its_identifier_in_both_directions(Ordering ordering) throws SQLException {
        StoreFixture fixture = setUp();
        long first = fixture.push(topic, Insertion.APPEND, prioritized("x", 0)).getFirst().id();
        fixture.push(topic, Insertion.APPEND, prioritized("x", 5));
        // The later task is running, e.g. because the earlier one committed late.
        fixture.claim(topic, node, Ordering.FIFO, 1);

        assertThat(ids(fixture.claim(topic, node, ordering, 10))).doesNotContain(first);
    }

    @ParameterizedTest
    @EnumSource(value = Ordering.class, names = "SINGULAR.*", mode = EnumSource.Mode.MATCH_ALL)
    void one_task_per_identifier_per_batch(Ordering ordering) throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, task("x"), task("y"), task("x"), task("y"), task("z"));

        assertThat(fixture.claim(topic, node, ordering, 10)).extracting(ClaimedTask::identifier).containsExactly("x", "y", "z");
    }

    @Test
    void fifo_by_priority_then_sequence_and_claims_count_attempts() throws SQLException {
        StoreFixture fixture = setUp();
        List<Long> pushed = fixture.push(topic, Insertion.APPEND, prioritized("low", 0), prioritized("high", 5), prioritized("low2", 0))
            .stream().map(p -> p.id()).toList();

        List<ClaimedTask> claimed = fixture.claim(topic, node, Ordering.FIFO, 2);

        assertThat(ids(claimed)).containsExactly(pushed.get(1), pushed.get(0));
        assertThat(claimed).extracting(ClaimedTask::attempt).containsOnly(1);
        assertThat(claimed.getFirst().identifier()).isEqualTo("high");
        assertThat(claimed.getFirst().claimedAt()).isNotNull();
        assertThat(fixture.view(pushed.get(1)).orElseThrow().nodeId()).isEqualTo(node);
    }

    @Test
    void a_claim_carries_payload_and_lineage() throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, new TaskStore.NewTask("x", "body", "v2", "ref", null, false, 0, null, 3L, 1L, TRACE));

        ClaimedTask claimed = fixture.claim(topic, node).getFirst();

        assertThat(claimed.payload()).isEqualTo("body");
        assertThat(claimed.payloadType()).isEqualTo("v2");
        assertThat(claimed.reference()).isEqualTo("ref");
        assertThat(claimed.parentId()).isEqualTo(3L);
        assertThat(claimed.rootId()).isEqualTo(1L);
        assertThat(claimed.createdAt()).isNotNull();
        assertThat(claimed.traceContext()).isEqualTo(TRACE);
    }

    private static final String TRACE = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    @Test
    void concurrent_claimers_never_share_a_task() throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 50));
        try (StoreTransaction first = fixture.store().begin(); StoreTransaction second = fixture.store().begin()) {
            List<ClaimedTask> a = fixture.store().claim(first, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, 30, null)).tasks();
            List<ClaimedTask> b = fixture.store().claim(second, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, 30, null)).tasks();
            first.commit();
            second.commit();

            Set<Long> overlap = new HashSet<>(ids(a));
            overlap.retainAll(ids(b));
            assertThat(overlap).isEmpty();
            assertThat(a.size() + b.size()).isEqualTo(50);
        }
    }

    @ParameterizedTest
    @EnumSource(value = Ordering.class, names = "SINGULAR.*", mode = EnumSource.Mode.MATCH_ALL)
    void concurrent_claimers_never_take_the_same_identifier(Ordering ordering) throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, task("x"), task("x"), task("y"), task("y"));
        try (StoreTransaction first = fixture.store().begin(); StoreTransaction second = fixture.store().begin()) {
            List<ClaimedTask> a = fixture.store().claim(first, new TaskStore.ClaimRequest(topic, node, ordering, 10, null)).tasks();
            List<ClaimedTask> b = fixture.store().claim(second, new TaskStore.ClaimRequest(topic, node, ordering, 10, null)).tasks();
            first.commit();
            second.commit();

            assertThat(a).extracting(ClaimedTask::identifier).containsExactly("x", "y");
            assertThat(b).isEmpty();
        }
    }
}
