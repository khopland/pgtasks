package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-25: the housekeeping leader requeued other nodes' tasks by its own expiry. A
 * worker with a longer one, alive and heartbeating at its own pace, lost its running task to the
 * leader's shorter expiry; the task ran twice at once and was dead-lettered as "Expired 3 times:
 * the node running it stopped every time", though the node never stopped.
 */
class NodeExpiryTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private EngineFixture fixture;

    @AfterEach
    void stop() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @Test
    void a_leader_with_a_shorter_expiry_leaves_a_live_node_its_tasks(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        fixture.createTopics(WORK.name());
        TaskEngine leader = fixture.start("leader", List.of(), settings -> settings.withHeartbeat(Duration.ofMillis(100), Duration.ofSeconds(1)));
        EngineFixture.await("leadership", Duration.ofSeconds(5), leader::leadsHousekeeping);
        AtomicInteger running = new AtomicInteger(), mostAtOnce = new AtomicInteger(), runs = new AtomicInteger();
        Topic<String> work = Topic.of(WORK, batch -> batch.decide(task -> {
            runs.incrementAndGet();
            mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
            try {
                Thread.sleep(4_000);
            } finally {
                running.decrementAndGet();
            }
            return Decision.success();
        }));
        TaskEngine worker = fixture.start("worker", List.of(work), settings -> settings.withHeartbeat(Duration.ofSeconds(2), Duration.ofHours(1)));
        long id = worker.tasks().independently().push(WORK, Insertion.APPEND, List.of(Creation.of("a", "x"))).getFirst().id();

        EngineFixture.await("the task to finish", Duration.ofSeconds(20), () -> !worker.admin().find(id).orElseThrow().state().equals("active")
            && !worker.admin().find(id).orElseThrow().state().equals("ready") && running.get() == 0);

        TaskView view = worker.admin().find(id).orElseThrow();
        assertThat(view.state()).isEqualTo("succeeded");
        assertThat(runs).as("ran once").hasValue(1);
        assertThat(mostAtOnce).hasValue(1);
        assertThat(leader.leadsHousekeeping()).as("the leader ran its expiry job all along").isTrue();
    }
}
