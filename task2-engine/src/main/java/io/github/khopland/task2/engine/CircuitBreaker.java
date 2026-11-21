package io.github.khopland.task2.engine;

import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.Map;

/** One topic's breaker on one node: see {@link Breaker}. */
final class CircuitBreaker {

    enum Permit {
        /** Claim as usual. */
        CLAIM,
        /** Open: claim a single task, whose failure won't count. */
        PROBE,
        /** Open: claim nothing until the next probe. */
        WAIT
    }

    enum Change { NONE, OPENED, CLOSED }

    private record Result(long taskId, boolean failed) {
    }

    private final Breaker settings;
    private final InstantSource clock;
    private final ArrayDeque<Result> recent = new ArrayDeque<>();
    private boolean open;
    private boolean probing;
    private Instant nextProbe;

    CircuitBreaker(Breaker settings, InstantSource clock) {
        this.settings = settings;
        this.clock = clock;
    }

    synchronized Permit permit() {
        if (!open) {
            return Permit.CLAIM;
        }
        if (probing || clock.instant().isBefore(nextProbe)) {
            return Permit.WAIT;
        }
        probing = true;
        return Permit.PROBE;
    }

    synchronized boolean isOpen() {
        return open;
    }

    /** When the next probe may go, if the breaker is open and no probe is out; else null. */
    synchronized Instant nextProbe() {
        return open && !probing ? nextProbe : null;
    }

    /**
     * Records a batch's results: true for a task that failed, false for one that succeeded;
     * tasks that were delayed, suspended or throttled aren't in it. A probe that succeeded
     * closes the breaker; one that failed waits for the next.
     */
    synchronized Change record(Map<Long, Boolean> results, boolean probe) {
        if (!settings.enabled()) {
            return Change.NONE;
        }
        if (probe) {
            probing = false;
            if (results.containsValue(false)) {
                open = false;
                recent.clear();
                return Change.CLOSED;
            }
            if (!results.isEmpty()) {
                nextProbe = clock.instant().plus(settings.probeEvery());
            }
            return Change.NONE;
        }
        if (open) {
            // Batches that were already running when it opened.
            return Change.NONE;
        }
        results.forEach((taskId, failed) -> {
            recent.addLast(new Result(taskId, failed));
            if (recent.size() > settings.window()) {
                recent.removeFirst();
            }
        });
        if (recent.size() < settings.window()) {
            return Change.NONE;
        }
        long failures = recent.stream().filter(Result::failed).count();
        long failingTasks = recent.stream().filter(Result::failed).map(Result::taskId).distinct().count();
        if (failures >= Math.ceil(settings.opensAt() * settings.window()) && failingTasks >= settings.minTasks()) {
            open = true;
            nextProbe = clock.instant().plus(settings.probeEvery());
            return Change.OPENED;
        }
        return Change.NONE;
    }

    /** A probe that claimed nothing or was handed back: the next one may go. */
    synchronized void probeReturned() {
        probing = false;
    }
}
