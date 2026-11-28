package io.github.khopland.pgtasks.circuitbreaker;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.List;

import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;

import org.jspecify.annotations.Nullable;

/** One topic's breaker on one node: see {@link CircuitBreaker}. Its settings have passed their keys' checks. */
final class TopicBreaker {

    enum Change { NONE, OPENED, CLOSED }

    /** An open breaker, for operators. */
    record Open(Instant openedAt, Instant nextProbe, int failures, int window) {
    }

    private record Result(long taskId, boolean failed) {
    }

    /** No probe is out. */
    private static final long NO_PROBE = -1;

    private final double opensAt;
    private final int window;
    private final int minTasks;
    private final Duration probeEvery;
    private final InstantSource clock;

    private final ArrayDeque<Result> recent = new ArrayDeque<>();
    private int failures;
    /** Read without the lock, so a closed breaker costs the claim path no more than this read. */
    private volatile boolean open;
    /** While open: when it opened, and when the next probe may go. */
    private Instant openedAt = Instant.MIN;
    private Instant nextProbe = Instant.MIN;
    /** A probe permit was handed out, and the claim hasn't said yet what it got. */
    private boolean probeClaiming;
    /** The task the last probe claimed, whose result alone opens or closes the breaker; {@link #NO_PROBE} if none is out. */
    private long probe = NO_PROBE;

    TopicBreaker(double opensAt, int window, int minTasks, Duration probeEvery, InstantSource clock) {
        this.opensAt = opensAt;
        this.window = window;
        this.minTasks = minTasks;
        this.probeEvery = probeEvery;
        this.clock = clock;
    }

    boolean isOpen() {
        return open;
    }

    Duration probeEvery() {
        return probeEvery;
    }

    /** "opens at 80 % of the last 50 results, from 5 tasks", for the logs. */
    String describe() {
        return "opens at " + Math.round(opensAt * 100) + " % of the last " + window + " results, from " + minTasks + " tasks";
    }

    /** While open, when it opened and when it probes next; null while closed. */
    synchronized @Nullable Open open() {
        return open ? new Open(openedAt, nextProbe, failures, window) : null;
    }

    /** Closed: any claim. Open: one task once the next probe is due, else wait until then. */
    ClaimGate.Permit permit() {
        if (!open) {
            return ClaimGate.Permit.ANY;
        }
        synchronized (this) {
            if (!open) {
                return ClaimGate.Permit.ANY;
            }
            Instant now = clock.instant();
            if (now.isBefore(nextProbe)) {
                return ClaimGate.Permit.waitFor(Duration.between(now, nextProbe), "breaker open");
            }
            // One probe per interval, even if the last hasn't come back: a probe that never
            // reports, such as one handed back on a stop, can't hold the topic for good.
            nextProbe = now.plus(probeEvery);
            probeClaiming = true;
            return ClaimGate.Permit.probe(1);
        }
    }

    /** What the claim after a permit got: the probe's task, or none, which lets the next probe go at once. */
    synchronized void claimed(List<Long> taskIds) {
        if (!probeClaiming) {
            return;
        }
        probeClaiming = false;
        if (taskIds.isEmpty()) {
            if (open) {
                nextProbe = clock.instant();
            }
        } else {
            probe = taskIds.getFirst();
        }
    }

    /**
     * A batch's results. Closed: failures and successes fill the window, and it opens when enough
     * of them failed, from enough tasks. Open: only the probe's result counts, matched by its task,
     * so results of batches that already ran when it opened don't: a success closes it, a failure
     * puts the next probe a full interval after it, and a failure of the task's own lets the next
     * probe go at once. Tasks that didn't fail and didn't finish (delayed, suspended) aren't
     * results, and nor are failures that are the task's own fault.
     */
    synchronized Change record(List<BatchListener.Completed> tasks) {
        if (open) {
            for (BatchListener.Completed task : tasks) {
                if (task.taskId() != probe) {
                    continue;
                }
                probe = NO_PROBE;
                if (task.succeeded()) {
                    closeNow();
                    return Change.CLOSED;
                }
                if (task.failure() == BatchListener.Failure.FAILED) {
                    Instant after = clock.instant().plus(probeEvery);
                    nextProbe = after.isAfter(nextProbe) ? after : nextProbe;
                } else if (task.failure() == BatchListener.Failure.TASK_FAULT) {
                    nextProbe = clock.instant();
                }
            }
            return Change.NONE;
        }
        for (BatchListener.Completed task : tasks) {
            boolean failed = task.failure() == BatchListener.Failure.FAILED;
            if (!failed && !task.succeeded()) {
                continue;
            }
            recent.addLast(new Result(task.taskId(), failed));
            if (failed) {
                failures++;
            }
            if (recent.size() > window && recent.removeFirst().failed()) {
                failures--;
            }
        }
        if (recent.size() < window || failures < Math.ceil(opensAt * window)) {
            return Change.NONE;
        }
        long failingTasks = recent.stream().filter(Result::failed).mapToLong(Result::taskId).distinct().count();
        if (failingTasks < minTasks) {
            return Change.NONE;
        }
        open = true;
        probeClaiming = false;
        probe = NO_PROBE;
        openedAt = clock.instant();
        nextProbe = openedAt.plus(probeEvery);
        return Change.OPENED;
    }

    /** Closes it by hand, as if a probe had succeeded; false if it wasn't open. */
    synchronized boolean close() {
        if (!open) {
            return false;
        }
        closeNow();
        return true;
    }

    private void closeNow() {
        open = false;
        recent.clear();
        failures = 0;
        probe = NO_PROBE;
        probeClaiming = false;
    }
}
