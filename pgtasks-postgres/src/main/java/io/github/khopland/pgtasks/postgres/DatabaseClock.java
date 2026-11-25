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
 * {@link #SLOW_SAMPLE} is ignored once the clock has a better one. Once sampled, the clock never
 * goes back: a sample that would move it back only holds it still until it catches up. The first
 * sample may: before it the clock is the node's own, and held at a node clock that runs ahead of
 * the database's, it would stand still for as long as the skew.
 */
final class DatabaseClock implements InstantSource {

    static final Duration SLOW_SAMPLE = Duration.ofSeconds(1);

    private record Anchor(Instant database, long nanos, boolean sampled) {

        Instant at(long nanos) {
            return database.plusNanos(nanos - this.nanos);
        }
    }

    /** The anchor and the latest time read, changed together, so the first sample can reset what was read before it. */
    private record State(Anchor anchor, Instant latest) {
    }

    private final LongSupplier nanoTime;
    private final AtomicReference<State> state;

    DatabaseClock() {
        this(InstantSource.system(), System::nanoTime);
    }

    DatabaseClock(InstantSource start, LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        Instant now = start.instant();
        this.state = new AtomicReference<>(new State(new Anchor(now, nanoTime.getAsLong(), false), now));
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
        if (roundTrip < 0) {
            return;
        }
        Anchor sampled = new Anchor(database, before + roundTrip / 2, true);
        state.updateAndGet(current -> {
            if (!current.anchor().sampled()) {
                // The node's clock until now: whatever was read from it doesn't hold this one back.
                return new State(sampled, sampled.at(nanoTime.getAsLong()));
            }
            return roundTrip > SLOW_SAMPLE.toNanos() ? current : new State(sampled, current.latest());
        });
    }

    @Override
    public Instant instant() {
        return state.updateAndGet(current -> {
            Instant now = current.anchor().at(nanoTime.getAsLong());
            return now.isAfter(current.latest()) ? new State(current.anchor(), now) : current;
        }).latest();
    }

    @Override
    public String toString() {
        return "DatabaseClock[" + instant() + "]";
    }
}
