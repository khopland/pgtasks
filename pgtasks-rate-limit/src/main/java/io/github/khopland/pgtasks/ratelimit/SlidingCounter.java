package io.github.khopland.pgtasks.ratelimit;

import java.time.Duration;

/**
 * A sliding-window counter made from the previous and current fixed windows: as the current
 * window advances, the previous one counts for less. Kept as plain values so a store can load,
 * lock, update and save it in one transaction.
 */
public final class SlidingCounter {

    /**
     * How many claims this gate may make now, or how long it should wait. A reservation remembers
     * the window it was counted in, so what goes unused is returned to that window.
     */
    public record Admission(long reserved, Duration delay, long windowStart, long windowMillis) {

        /** Whether nothing was reserved and the gate waits {@link #delay()}. */
        public boolean waits() {
            return reserved == 0;
        }
    }

    private long windowStart;
    private long previous;
    private long current;
    private boolean changed;

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

    /** Whether anything changed since it was loaded, so a store can skip saving it. */
    public boolean changed() {
        return changed;
    }

    /**
     * Reserves up to {@code wanted} of what the window allows at {@code now} (epoch milliseconds),
     * with {@code limit} claims allowed per window. The reservation is optimistic: a gate that ends
     * up claiming fewer tasks returns them with {@link #settle}.
     */
    public Admission admit(long now, long windowMillis, long limit, int wanted) {
        if (windowMillis < 1 || limit < 1 || wanted < 1) {
            throw new IllegalArgumentException("A window, limit and wanted count must be positive");
        }
        roll(now, windowMillis);
        long available = (long) Math.floor(limit - weight(now, windowMillis) + 1e-9);
        if (available >= 1) {
            long reserved = Math.min(available, wanted);
            current += reserved;
            changed = true;
            return new Admission(reserved, Duration.ZERO, windowStart, windowMillis);
        }
        return new Admission(0, Duration.ofMillis(waitMillis(now, windowMillis, limit)), windowStart, windowMillis);
    }

    /**
     * Settles a reservation with the number of tasks the claim actually got, at {@code now}. What
     * went unused comes off the window it was counted in: the current one, the previous one once
     * the window has moved on, or nowhere once that window no longer counts.
     */
    public void settle(Admission admission, int tasks, long now) {
        long unused = admission.reserved() - Math.max(0, tasks);
        if (unused <= 0) {
            return;
        }
        long windowMillis = admission.windowMillis();
        roll(now, windowMillis);
        if (admission.windowStart() == windowStart) {
            current = Math.max(0, current - unused);
            changed = true;
        } else if (admission.windowStart() == windowStart - windowMillis) {
            previous = Math.max(0, previous - unused);
            changed = true;
        }
    }

    /** Moves the previous/current pair onto the window containing {@code now}. */
    private void roll(long now, long windowMillis) {
        long start = Math.floorDiv(now, windowMillis) * windowMillis;
        if (start <= windowStart) {
            // Still in the stored window (or, after a longer window was configured, before it).
            return;
        }
        // The next window keeps the old count as its previous one; further on, or after the
        // window's length changed, nothing earlier still counts.
        previous = start == windowStart + windowMillis ? current : 0;
        current = 0;
        windowStart = start;
        changed = true;
    }

    /** The previous window's remaining share, plus all of the current one. */
    private double weight(long now, long windowMillis) {
        double fraction = Math.min(1.0, Math.max(0.0, (double) (now - windowStart) / windowMillis));
        return previous * (1 - fraction) + current;
    }

    /** How long until one slot is free, from the previous window's decay or the next boundary. */
    private long waitMillis(long now, long windowMillis, long limit) {
        long untilBoundary = Math.max(1, windowMillis - (now - windowStart));
        if (previous > 0 && current < limit) {
            double deficit = 1 + weight(now, windowMillis) - limit;
            long wait = (long) Math.ceil(deficit / ((double) previous / windowMillis));
            return Math.max(1, Math.min(wait, untilBoundary));
        }
        return untilBoundary;
    }
}
