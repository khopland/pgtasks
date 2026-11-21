package org.example.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.example.task2.Creation;
import org.example.task2.Decision;
import org.example.task2.Insertion;
import org.example.task2.TopicRef;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Operators' runtime overrides of rate limits: on the in-memory store, and the Phase 3 exit test on Postgres. */
class RateLimitTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> TOPIC = TopicRef.of("topic", String.class);
    private static final RateLimit ONE_A_MINUTE = RateLimit.perSecond("downstream", 1.0 / 60, 1);

    private final MemoryEngines engines = new MemoryEngines();
    private EngineFixture fixture;

    @AfterEach
    void tearDown() {
        engines.close();
        if (fixture != null) {
            fixture.close();
        }
    }

    private static List<Topic<?>> limited(AtomicInteger runs) {
        return List.of(Topic.of(TOPIC, batch -> batch.decide(task -> {
            runs.incrementAndGet();
            return Decision.success();
        })).with(TopicSettings.defaults().withRateLimit(ONE_A_MINUTE)));
    }

    private static double rate(TaskEngine engine) {
        return engine.limits().find(ONE_A_MINUTE.name()).orElseThrow().effective().perSecond();
    }

    @Test
    void an_override_changes_the_limit_on_every_node_and_a_reset_restores_it() throws Exception {
        TaskEngine nodeA = engines.start("node-a", limited(new AtomicInteger()));
        TaskEngine nodeB = engines.start("node-b", limited(new AtomicInteger()));

        RateLimits.Limit overridden = nodeA.limits().override(ONE_A_MINUTE.name(), Rate.perSecond(50), null, null, "ops", 0L);

        assertThat(overridden.version()).isEqualTo(1);
        assertThat(overridden.configured()).isEqualTo(ONE_A_MINUTE);
        assertThat(overridden.effective()).isEqualTo(ONE_A_MINUTE.withRate(50, 1));
        EngineFixture.await("node B to apply it", Duration.ofSeconds(1), () -> rate(nodeB) == 50);

        RateLimits.Limit reset = nodeB.limits().reset(ONE_A_MINUTE.name(), "ops", 1L);

        assertThat(reset.version()).isEqualTo(2);
        assertThat(reset.effective()).isEqualTo(ONE_A_MINUTE);
        EngineFixture.await("node A to apply the reset", Duration.ofSeconds(1), () -> rate(nodeA) == ONE_A_MINUTE.perSecond());
    }

    @Test
    void a_change_based_on_an_old_version_is_refused() throws Exception {
        TaskEngine engine = engines.start("node-a", limited(new AtomicInteger()));
        engine.limits().override(ONE_A_MINUTE.name(), Rate.perSecond(50), null, null, "first operator", 0L);

        assertThatThrownBy(() -> engine.limits().override(ONE_A_MINUTE.name(), Rate.perSecond(5), null, null, "second operator", 0L))
            .isInstanceOf(VersionConflictException.class);
        assertThat(rate(engine)).isEqualTo(50);
    }

    @Test
    void an_override_lapses_by_itself() throws Exception {
        TaskEngine engine = engines.start("node-a", limited(new AtomicInteger()));
        engine.limits().override(ONE_A_MINUTE.name(), null, 10, Duration.ofHours(2), "ops", null);
        assertThat(engine.limits().find(ONE_A_MINUTE.name()).orElseThrow().effective()).isEqualTo(ONE_A_MINUTE.withRate(ONE_A_MINUTE.perSecond(), 10));

        engines.clock.advance(Duration.ofHours(2).plusSeconds(1));

        assertThat(engine.limits().find(ONE_A_MINUTE.name()).orElseThrow().effective()).isEqualTo(ONE_A_MINUTE);
    }

    @Test
    void an_override_needs_a_value_and_a_limit_some_node_configured() throws Exception {
        TaskEngine engine = engines.start("node-a", limited(new AtomicInteger()));
        assertThatThrownBy(() -> engine.limits().override(ONE_A_MINUTE.name(), null, null, null, "ops", null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> engine.limits().override("nobody-uses-this", Rate.perSecond(1), null, null, "ops", null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Rate.perSecond(0)).as("a rate of 0 is not a pause").isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The Phase 3 exit test: a limit change reaches every node within about a second. Heartbeats
     * are an hour apart, so only the notification can carry it; the workers are waiting a minute
     * for their next permit, and the change has to cut that wait short.
     */
    @Test
    void a_limit_change_reaches_every_node_within_a_second(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        AtomicInteger runs = new AtomicInteger();
        List<TaskEngine> nodes = List.of(
            fixture.start("node-a", limited(runs), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))),
            fixture.start("node-b", limited(runs), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))),
            fixture.start("node-c", limited(runs), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(10))));
        nodes.getFirst().tasks().independently().push(TOPIC, Insertion.APPEND,
            java.util.stream.IntStream.range(0, 30).mapToObj(index -> Creation.of("t" + index, "x")).toList());
        EngineFixture.await("the burst of one", Duration.ofSeconds(5), () -> runs.get() == 1);
        Thread.sleep(300);
        assertThat(runs.get()).as("everyone waits for the next permit").isEqualTo(1);

        // Only a notification carries the change here, so every node must be listening first.
        EngineFixture.await("every node listening", Duration.ofSeconds(5), () -> nodes.stream().allMatch(node -> {
            try {
                return node.health().notifications() == EngineHealth.Notifications.CONNECTED;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }));
        long started = System.nanoTime();
        nodes.get(1).limits().override(ONE_A_MINUTE.name(), Rate.perSecond(1000), 100, null, "ops", 0L);
        for (TaskEngine node : nodes) {
            EngineFixture.await(node + " to apply the override", Duration.ofSeconds(1), () -> rate(node) == 1000);
        }
        fixture.awaitQueueEmpty(Duration.ofSeconds(2));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(runs.get()).isEqualTo(30);
        assertThat(elapsed).as("from the change to an empty queue").isLessThan(Duration.ofSeconds(2));
    }
}
