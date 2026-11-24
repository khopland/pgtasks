package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.Objects;

/**
 * Work the engine runs on a fixed delay while it runs: on every node, or only on the node that
 * holds the housekeeping lease. The engine's own housekeeping (expiry, promotion, retention,
 * count snapshots, the orphan sweep) is made of these too. An extension adds its own with
 * {@link ExtensionContext#job}.
 *
 * <p>A job that throws is logged and runs again after its interval. Leader jobs must be safe to
 * run on two nodes at once: for at most one heartbeat a hung leader may not know yet that it lost
 * the lease. A job that loops over batches should stop when {@link ExtensionContext#leading()}
 * turns false.
 */
public interface HousekeepingJob {

    /** Where a job runs. */
    enum Scope {
        /** On every node that runs, such as a sweep of what this node holds. */
        EVERY_NODE,
        /** Only on the node holding the housekeeping lease: cluster-wide work, done once. */
        LEADER
    }

    @FunctionalInterface
    interface Action {
        void run() throws Exception;
    }

    /** For the logs, such as "promotion". */
    String name();

    Scope scope();

    /** The delay between the end of one run and the start of the next. */
    Duration interval();

    void run() throws Exception;

    static HousekeepingJob leader(String name, Duration interval, Action action) {
        return of(name, Scope.LEADER, interval, action);
    }

    static HousekeepingJob everyNode(String name, Duration interval, Action action) {
        return of(name, Scope.EVERY_NODE, interval, action);
    }

    private static HousekeepingJob of(String name, Scope scope, Duration interval, Action action) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(action, "action");
        if (!interval.isPositive()) {
            throw new IllegalArgumentException("The interval of job " + name + " must be positive");
        }
        return new HousekeepingJob() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Scope scope() {
                return scope;
            }

            @Override
            public Duration interval() {
                return interval;
            }

            @Override
            public void run() throws Exception {
                action.run();
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }
}
