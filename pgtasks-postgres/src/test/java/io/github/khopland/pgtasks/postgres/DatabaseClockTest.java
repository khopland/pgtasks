package io.github.khopland.pgtasks.postgres;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseClockTest {

    private static final Instant NODE = Instant.parse("2026-11-24T12:00:00Z");
    private static final Instant DATABASE = NODE.plus(Duration.ofMinutes(5));

    private final AtomicLong nanos = new AtomicLong(1_000_000_000L);
    private final DatabaseClock clock = new DatabaseClock(() -> NODE, nanos::get);

    @Test
    void is_the_node_clock_until_sampled() {
        assertThat(clock.instant()).isEqualTo(NODE);
        nanos.addAndGet(Duration.ofSeconds(3).toNanos());
        assertThat(clock.instant()).isEqualTo(NODE.plusSeconds(3));
    }

    @Test
    void follows_the_database_from_halfway_through_the_sample() {
        long before = nanos.get();
        nanos.addAndGet(Duration.ofMillis(20).toNanos());
        clock.sample(DATABASE, before, nanos.get());

        assertThat(clock.instant()).isEqualTo(DATABASE.plusMillis(10));
        nanos.addAndGet(Duration.ofMinutes(1).toNanos());
        assertThat(clock.instant()).as("moved on by the monotonic clock, whatever the wall clock does").isEqualTo(DATABASE.plusMillis(10).plusSeconds(60));
    }

    @Test
    void never_goes_back() {
        clock.sample(DATABASE, nanos.get(), nanos.get());
        Instant read = clock.instant();

        clock.sample(DATABASE.minusSeconds(2), nanos.get(), nanos.get());

        assertThat(clock.instant()).isEqualTo(read);
        nanos.addAndGet(Duration.ofSeconds(3).toNanos());
        assertThat(clock.instant()).isEqualTo(DATABASE.plusSeconds(1));
    }

    @Test
    void ignores_a_slow_sample_once_it_has_a_good_one() {
        clock.sample(DATABASE, nanos.get(), nanos.get());
        long before = nanos.get();
        nanos.addAndGet(DatabaseClock.SLOW_SAMPLE.plusSeconds(1).toNanos());

        clock.sample(DATABASE.plus(Duration.ofHours(1)), before, nanos.get());

        assertThat(clock.instant()).isEqualTo(DATABASE.plus(DatabaseClock.SLOW_SAMPLE).plusSeconds(1));
    }

    @Test
    void takes_a_slow_sample_when_it_has_none() {
        long before = nanos.get();
        nanos.addAndGet(Duration.ofSeconds(4).toNanos());

        clock.sample(DATABASE, before, nanos.get());

        assertThat(clock.instant()).isEqualTo(DATABASE.plusSeconds(2));
    }
}
