package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Whether this node's heartbeat is so late that other nodes may soon take its tasks over. Once a
 * node has gone {@code expiry} without a heartbeat, the housekeeping leader requeues its tasks,
 * and another node may run them while this one still does: two runs of one task at once, two
 * tasks of one identifier at once under a SINGULAR ordering. A heartbeat can be late while the
 * node otherwise works, when the pool has no connection for it, in a long GC or VM pause, or when
 * only the database is out of reach. So a little before the expiry, the node fences itself: it
 * stops claiming and cancels its running handlers, whose tasks go back without counting an
 * attempt. A handler that ignores cancellation still runs, which the fence can't prevent.
 *
 * <p>Measured on the JVM's monotonic clock from when the last successful heartbeat began, which
 * is no later than the time the store recorded for it, so the fence errs early.
 */
final class HeartbeatFence {

    private final long fenceAfter;
    private final LongSupplier nanoTime;
    private volatile long lastBeat;

    HeartbeatFence(Duration heartbeat, Duration expiry, LongSupplier nanoTime) {
        this.fenceAfter = fenceAfter(heartbeat, expiry).toNanos();
        this.nanoTime = nanoTime;
        this.lastBeat = nanoTime.getAsLong();
    }

    /**
     * How long after its last heartbeat a node fences itself: the expiry, less four heartbeats (the
     * leader's expiry job runs once a heartbeat, and a beat takes time of its own), or less half
     * of what lies between a heartbeat and the expiry when they are close.
     */
    static Duration fenceAfter(Duration heartbeat, Duration expiry) {
        Duration margin = heartbeat.multipliedBy(4);
        Duration half = expiry.minus(heartbeat).dividedBy(2);
        return expiry.minus(margin.compareTo(half) < 0 ? margin : half);
    }

    /** How often the watchdog checks: often enough that it fences well before the expiry. */
    Duration checkInterval() {
        Duration room = Duration.ofNanos(fenceAfter).dividedBy(8);
        Duration floor = Duration.ofMillis(10);
        return room.compareTo(floor) < 0 ? floor : room.compareTo(Duration.ofSeconds(5)) > 0 ? Duration.ofSeconds(5) : room;
    }

    long now() {
        return nanoTime.getAsLong();
    }

    /** A heartbeat that began at {@code began} (see {@link #now()}) succeeded. */
    void beat(long began) {
        if (began - lastBeat > 0) {
            lastBeat = began;
        }
    }

    boolean fenced() {
        return nanoTime.getAsLong() - lastBeat > fenceAfter;
    }

    /** How long since the last successful heartbeat began. */
    Duration silence() {
        return Duration.ofNanos(nanoTime.getAsLong() - lastBeat);
    }
}
