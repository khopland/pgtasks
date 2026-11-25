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

    /**
     * Review 2026-11-25: the clock started at the node's own time and never went back, so on a node
     * whose clock ran an hour ahead of the database's it stood still for that hour after the first
     * sample. Heartbeat ages read zero, so health couldn't see a heartbeat stop, and a lost lease
     * still looked held.
     */
    @Test
    void the_first_sample_moves_it_back_to_the_database() {
        DatabaseClock ahead = new DatabaseClock(() -> DATABASE.plus(Duration.ofHours(1)), nanos::get);
        assertThat(ahead.instant()).isEqualTo(DATABASE.plus(Duration.ofHours(1)));

        ahead.sample(DATABASE, nanos.get(), nanos.get());

        assertThat(ahead.instant()).isEqualTo(DATABASE);
        nanos.addAndGet(Duration.ofMinutes(30).toNanos());
        assertThat(ahead.instant()).as("moves on at once, not after the hour").isEqualTo(DATABASE.plus(Duration.ofMinutes(30)));
        ahead.sample(DATABASE.plus(Duration.ofMinutes(29)), nanos.get(), nanos.get());
        assertThat(ahead.instant()).as("later samples still never move it back").isEqualTo(DATABASE.plus(Duration.ofMinutes(30)));
    }
}
