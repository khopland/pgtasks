package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-25: a batch past its deadline failed every task in it, its attempt counted. With
 * {@code decideConcurrently}, one remote call that hung failed the tasks already done, and their
 * side effects ran again on the retry; tasks whose decider never started were counted as failed
 * too.
 */
class DeadlineTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    private static final TopicRef<String> NEXT = TopicRef.of("next", String.class);
    private static final Duration DEADLINE = Duration.ofMillis(300);

    private final MemoryEngines engines = new MemoryEngines();
    private final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
    private final Set<String> hangOnce = ConcurrentHashMap.newKeySet();

    @AfterEach
    void tearDown() {
        engines.close();
    }

    private Decision run(io.github.khopland.pgtasks.Work<String> work) throws InterruptedException {
        runs.computeIfAbsent(work.task().identifier(), ignored -> new AtomicInteger()).incrementAndGet();
        if (hangOnce.remove(work.task().identifier())) {
            Thread.sleep(Duration.ofMinutes(5));
        }
        work.push(NEXT, work.task().identifier(), "from " + work.task().identifier());
        return Decision.success();
    }

    private TaskView view(TaskTester tester, long id) {
        return tester.find(id).orElseThrow();
    }

    private Topic<String> next() {
        return Topic.of(NEXT, batch -> batch.decide(task -> Decision.success()));
    }

    @Test
    void decisions_made_before_the_deadline_stand_and_only_the_task_still_running_fails() throws Exception {
        hangOnce.add("slow");
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(WORK, batch -> batch.decideConcurrently(5, this::run))
            .with(TopicSettings.defaults().withBatchSize(5).withBatchDeadline(DEADLINE)), next()));
        TaskTester tester = engines.tester();
        List<Long> ids = engine.tasks().independently().push(WORK, Insertion.APPEND,
            List.of("a", "b", "slow", "c", "d").stream().map(id -> Creation.of(id, "x")).toList()).stream().map(pushed -> pushed.id()).toList();

        tester.drain(WORK);

        for (int index : new int[] {0, 1, 3, 4}) {
            TaskView view = view(tester, ids.get(index));
            assertThat(view.state()).as(view.identifier()).isEqualTo("succeeded");
            assertThat(view.attempt()).as(view.identifier() + " kept its decision").isEqualTo(1);
            assertThat(runs.get(view.identifier())).as(view.identifier() + " ran once").hasValue(1);
        }
        TaskView slow = view(tester, ids.get(2));
        assertThat(slow.state()).isEqualTo("succeeded");
        assertThat(slow.attempt()).as("its first run failed at the deadline").isEqualTo(2);
        assertThat(tester.pushedIdentifiers(NEXT)).as("each task's downstream work, once").containsExactlyInAnyOrder("a", "b", "slow", "c", "d");
    }

    @Test
    void tasks_whose_decider_never_started_go_back_without_an_attempt() throws Exception {
        hangOnce.add("b");
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(WORK, batch -> batch.decide(this::run))
            .with(TopicSettings.defaults().withBatchSize(4).withBatchDeadline(DEADLINE)), next()));
        TaskTester tester = engines.tester();
        List<Long> ids = engine.tasks().independently().push(WORK, Insertion.APPEND,
            List.of("a", "b", "c", "d").stream().map(id -> Creation.of(id, "x")).toList()).stream().map(pushed -> pushed.id()).toList();

        tester.drain(WORK);

        assertThat(view(tester, ids.get(0)).attempt()).as("a, decided before the deadline").isEqualTo(1);
        assertThat(view(tester, ids.get(1)).attempt()).as("b, running at the deadline").isEqualTo(2);
        assertThat(view(tester, ids.get(2)).attempt()).as("c, never started").isEqualTo(1);
        assertThat(view(tester, ids.get(3)).attempt()).as("d, never started").isEqualTo(1);
        assertThat(ids).allSatisfy(id -> assertThat(view(tester, id).state()).isEqualTo("succeeded"));
        assertThat(runs.get("a")).hasValue(1);
        assertThat(tester.pushedIdentifiers(NEXT)).containsExactlyInAnyOrder("a", "b", "c", "d");
    }

    @Test
    void a_handler_that_runs_out_of_time_before_deciding_fails_every_task() throws Exception {
        AtomicInteger batches = new AtomicInteger();
        TaskEngine engine = engines.start("node-a", List.of(Topic.of(WORK, batch -> {
            if (batches.incrementAndGet() == 1) {
                Thread.sleep(Duration.ofMinutes(5)); // a lookup for the whole batch, before any decider
            }
            return batch.decide(this::run);
        }).with(TopicSettings.defaults().withBatchSize(2).withBatchDeadline(DEADLINE)), next()));
        TaskTester tester = engines.tester();
        List<Long> ids = engine.tasks().independently().push(WORK, Insertion.APPEND,
            List.of(Creation.of("a", "x"), Creation.of("b", "x"))).stream().map(pushed -> pushed.id()).toList();

        tester.drain(WORK);

        assertThat(ids).as("counted, so a handler that always runs out of time can't hand its tasks back forever")
            .allSatisfy(id -> assertThat(view(tester, id).attempt()).isEqualTo(2));
    }
}
