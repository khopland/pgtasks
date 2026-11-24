package io.github.khopland.pgtasks.ratelimit;

import java.time.Duration;

/**
 * A sliding-window counter made from the previous and current fixed windows: as the current
 * window advances, the previous one counts for less. Kept as plain values so a store can load,
 * lock, update and save it in one transaction.
 */
public final class SlidingCounter {

    /** How many claims this gate may make now, or how long it should wait. */
    public record Admission(long reserved, Duration delay) {
        boolean waits() {
            return reserved == 0;
        }
    }

    private long windowStart;
    private long previous;
    private long current;

    /** A counter that starts empty in the window containing the first admission. */
    public SlidingCounter() {
    }

    /** A counter loaded from shared storage. */
    public SlidingCounter(long windowStart, long previous, long current) {
        this.windowStart = windowStart;
        this.previous = previous;
        this.current = current;
    }

    public long windowStart() {
        return windowStart;
    }

    public long previous() {
        return previous;
    }

    public long current() {
        return current;
    }

    /**
     * Reserves up to what the window allows at {@code now} (epoch milliseconds). {@code limit} is
     * the number of claims allowed per window and {@code burst} the extra headroom that can be
     * spent quickly. The reservation is optimistic: a gate that ends up claiming fewer tasks
     * returns them with {@link #claimed}.
     */
    public Admission admit(long now, long windowMillis, double limit, double burst) {
        roll(now, windowMillis);
        double capacity = limit + burst;
        double available = Math.floor(capacity - weight(now, windowMillis) + 1e-9);
        if (available >= 1) {
            long reserved = (long) Math.min(available, Integer.MAX_VALUE);
            current += reserved;
            return new Admission(reserved, Duration.ZERO);
        }
        return new Admission(0, Duration.ofMillis(waitMillis(now, windowMillis, capacity)));
    }

    /** Settles a reservation with what the claim actually got. */
    public void claimed(int tasks, long reserved) {
        current = Math.max(0, current + tasks - reserved);
    }

    /** Moves the previous/current pair onto the window containing {@code now}. */
    private void roll(long now, long windowMillis) {
        long bucket = Math.floorDiv(now, windowMillis);
        long held = Math.floorDiv(windowStart, windowMillis);
        if (bucket <= held) {
            return;
        }
        if (bucket == held + 1) {
            previous = current;
        } else {
            previous = 0;
        }
        current = 0;
        windowStart = bucket * windowMillis;
    }

    /** The previous window's remaining share, plus all of the current one. */
    private double weight(long now, long windowMillis) {
        double fraction = Math.min(1.0, Math.max(0.0, (double) (now - windowStart) / windowMillis));
        return previous * (1 - fraction) + current;
    }

    /** How long until one slot is free, from the previous window's decay or the next boundary. */
    private long waitMillis(long now, long windowMillis, double capacity) {
        double deficit = 1 + weight(now, windowMillis) - capacity;
        if (previous > 0) {
            long wait = (long) Math.ceil(deficit / ((double) previous / windowMillis));
            return Math.max(1, wait);
        }
        return Math.max(1, windowMillis - (now - windowStart));
    }
}
