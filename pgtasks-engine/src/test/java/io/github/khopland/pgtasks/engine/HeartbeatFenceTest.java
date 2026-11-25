package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HeartbeatFenceTest {

    @Test
    void fences_four_heartbeats_before_the_expiry_or_halfway_when_they_are_close() {
        assertThat(HeartbeatFence.fenceAfter(Duration.ofSeconds(15), Duration.ofMinutes(10))).isEqualTo(Duration.ofSeconds(540));
        assertThat(HeartbeatFence.fenceAfter(Duration.ofSeconds(10), Duration.ofSeconds(15))).isEqualTo(Duration.ofMillis(12_500));
    }

    @Test
    void is_fenced_once_no_heartbeat_got_through_for_long_enough() {
        AtomicLong nanos = new AtomicLong();
        HeartbeatFence fence = new HeartbeatFence(Duration.ofSeconds(15), Duration.ofMinutes(10), nanos::get);

        nanos.set(Duration.ofSeconds(540).toNanos());
        assertThat(fence.fenced()).isFalse();
        nanos.incrementAndGet();
        assertThat(fence.fenced()).isTrue();

        fence.beat(Duration.ofSeconds(530).toNanos());
        assertThat(fence.fenced()).as("measured from when the beat began").isFalse();
        fence.beat(Duration.ofSeconds(10).toNanos());
        assertThat(fence.silence()).as("an older beat finishing late doesn't move it back").isEqualTo(Duration.ofSeconds(10).plusNanos(1));
    }

    @Test
    void checks_often_enough_to_fence_well_before_the_expiry() {
        assertThat(new HeartbeatFence(Duration.ofSeconds(15), Duration.ofMinutes(10), System::nanoTime).checkInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(new HeartbeatFence(Duration.ofMillis(100), Duration.ofSeconds(1), System::nanoTime).checkInterval()).isEqualTo(Duration.ofMillis(75));
    }
}
