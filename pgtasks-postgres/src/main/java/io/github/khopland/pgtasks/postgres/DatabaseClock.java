package io.github.khopland.pgtasks.postgres;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * The database's time, as this node reads it without asking: the last sample of the database's
 * {@code clock_timestamp()}, moved on by the JVM's monotonic clock. The engine compares this clock
 * with times the database stamped (when a task first failed, when it finished) and keeps its
 * housekeeping lease by it, so a node whose wall clock is off, or is stepped by NTP, still agrees
 * with the database. Until the first sample it is the system clock.
 *
 * <p>Each heartbeat and node registration takes a sample, placed halfway through its round trip,
 * so it is off by at most half of one. A sample whose round trip took longer than
 * {@link #SLOW_SAMPLE} is ignored once the clock has a better one. The clock never goes back:
 * a sample that would move it back only holds it still until it catches up.
 */
final class DatabaseClock implements InstantSource {

    static final Duration SLOW_SAMPLE = Duration.ofSeconds(1);

    private record Anchor(Instant database, long nanos, boolean sampled) {
    }

    private final LongSupplier nanoTime;
    private volatile Anchor anchor;
    private final AtomicReference<Instant> latest;

    DatabaseClock() {
        this(InstantSource.system(), System::nanoTime);
    }

    DatabaseClock(InstantSource start, LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        Instant now = start.instant();
        this.anchor = new Anchor(now, nanoTime.getAsLong(), false);
        this.latest = new AtomicReference<>(now);
    }

    /** The JVM's monotonic clock, to read before and after the statement that samples the database's. */
    long nanos() {
        return nanoTime.getAsLong();
    }

    /**
     * @param database the database's {@code clock_timestamp()} during the statement
     * @param before   {@link #nanos()} before the statement was sent
     * @param after    {@link #nanos()} once its result was read
     */
    void sample(Instant database, long before, long after) {
        long roundTrip = after - before;
        if (roundTrip < 0 || (roundTrip > SLOW_SAMPLE.toNanos() && anchor.sampled())) {
            return;
        }
        anchor = new Anchor(database, before + roundTrip / 2, true);
    }

    @Override
    public Instant instant() {
        Anchor current = anchor;
        Instant now = current.database().plusNanos(nanoTime.getAsLong() - current.nanos());
        return latest.accumulateAndGet(now, (previous, next) -> next.isAfter(previous) ? next : previous);
    }

    @Override
    public String toString() {
        return "DatabaseClock[" + instant() + "]";
    }
}
