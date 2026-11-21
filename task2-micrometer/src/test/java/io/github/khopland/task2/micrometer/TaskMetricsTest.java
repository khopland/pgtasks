package io.github.khopland.task2.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.task2.admin.QueueCounts;
import io.github.khopland.task2.admin.TopicSnapshot;
import io.github.khopland.task2.spi.ManualClock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TaskMetricsTest {

    private static final Instant NOW = Instant.parse("2026-11-21T10:00:00Z");

    private final ManualClock clock = new ManualClock(NOW);
    private final List<TopicSnapshot> snapshots = new CopyOnWriteArrayList<>();
    private final AtomicInteger reads = new AtomicInteger();
    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final TaskMetrics metrics = new TaskMetrics(() -> {
        reads.incrementAndGet();
        return List.copyOf(snapshots);
    }, Duration.ofSeconds(10), clock);

    private static TopicSnapshot snapshot(String topic, long ready, Instant oldestReady, Instant takenAt) {
        return new TopicSnapshot(topic, new QueueCounts(1, 2, ready, 4, 5, 6, 7, null, oldestReady, 3, NOW.minusSeconds(600)), takenAt);
    }

    private double gauge(String name, String topic, String... tags) {
        return registry.get(name).tag("topic", topic).tags(tags).gauge().value();
    }

    @Test
    @DisplayName("Gauges per topic and state, and the age of the oldest ready task and of the snapshot")
    void gauges_show_the_last_snapshot() {
        snapshots.add(snapshot("orders", 3, NOW.minusSeconds(90), NOW.minusSeconds(20)));
        metrics.bindTo(registry);

        assertThat(gauge(TaskMetrics.QUEUE, "orders", "state", "ready")).isEqualTo(3);
        assertThat(gauge(TaskMetrics.QUEUE, "orders", "state", "failed")).isEqualTo(6);
        assertThat(gauge(TaskMetrics.QUEUE, "orders", "state", "due")).isEqualTo(7);
        assertThat(gauge(TaskMetrics.OLDEST_READY, "orders")).isEqualTo(90);
        assertThat(gauge(TaskMetrics.SNAPSHOT_AGE, "orders")).isEqualTo(20);
        assertThat(gauge(TaskMetrics.FAILING, "orders")).isEqualTo(3);
        assertThat(gauge(TaskMetrics.OLDEST_FAILING, "orders")).isEqualTo(600);

        clock.advance(Duration.ofSeconds(5));
        assertThat(gauge(TaskMetrics.OLDEST_READY, "orders")).isEqualTo(95);
    }

    @Test
    @DisplayName("Snapshots are read at most once per refresh, and new topics get gauges")
    void snapshots_are_read_once_per_refresh_and_new_topics_appear() {
        snapshots.add(snapshot("orders", 3, null, NOW));
        metrics.bindTo(registry);
        gauge(TaskMetrics.QUEUE, "orders", "state", "ready");
        gauge(TaskMetrics.QUEUE, "orders", "state", "active");
        assertThat(reads).hasValue(1);
        assertThat(gauge(TaskMetrics.OLDEST_READY, "orders")).isZero();

        snapshots.clear();
        snapshots.add(snapshot("orders", 8, NOW, NOW));
        snapshots.add(snapshot("receipts", 1, NOW, NOW));
        clock.advance(Duration.ofSeconds(11));

        assertThat(gauge(TaskMetrics.QUEUE, "orders", "state", "ready")).isEqualTo(8);
        assertThat(reads).hasValue(2);
        assertThat(gauge(TaskMetrics.QUEUE, "receipts", "state", "ready")).isEqualTo(1);
    }

    @Test
    @DisplayName("Started before the first snapshot, the gauges appear with a later scrape")
    void topics_appear_when_bound_before_the_first_snapshot() {
        metrics.bindTo(registry);
        assertThat(registry.find(TaskMetrics.QUEUE).gauges()).isEmpty();
        assertThat(registry.get(TaskMetrics.TOPICS).gauge().value()).isZero();

        snapshots.add(snapshot("orders", 3, null, NOW));
        clock.advance(Duration.ofSeconds(11));

        assertThat(registry.get(TaskMetrics.TOPICS).gauge().value()).isEqualTo(1);
        assertThat(gauge(TaskMetrics.QUEUE, "orders", "state", "ready")).isEqualTo(3);
    }

    @Test
    @DisplayName("A failed read keeps the last counts")
    void a_failed_read_keeps_the_last_counts() {
        AtomicInteger calls = new AtomicInteger();
        TaskMetrics failing = new TaskMetrics(() -> {
            if (calls.incrementAndGet() > 1) {
                throw new IllegalStateException("database down");
            }
            return List.of(snapshot("orders", 3, null, NOW));
        }, Duration.ofSeconds(10), clock);
        failing.bindTo(registry);
        clock.advance(Duration.ofSeconds(11));

        assertThat(gauge(TaskMetrics.QUEUE, "orders", "state", "ready")).isEqualTo(3);
        assertThat(calls).hasValue(2);
    }
}
