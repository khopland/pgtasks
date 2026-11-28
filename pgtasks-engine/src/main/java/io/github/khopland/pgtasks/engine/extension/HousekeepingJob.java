package io.github.khopland.pgtasks.engine.extension;

import java.time.Duration;
import java.util.Objects;

/**
 * Work the engine runs on a fixed delay while it runs: on every node, or on one node at a time.
 * The engine's own housekeeping (expiry, promotion, retention, count snapshots, the orphan sweep)
 * is made of these too. An extension adds its own with {@link ExtensionContext#job}.
 *
 * <p>A leader job runs on the node that holds its lease, a lease of its own named after its
 * extension and itself ({@code <extension>/<job>}, such as {@code Pause/pause cleanup}), which
 * only the nodes that have the job ask for: one application's extension runs its leader jobs even
 * when another application, without that extension, shares the schema and runs the engine's
 * housekeeping, and two extensions' jobs of one name don't share a lease. The nodes' leases show
 * in {@code pgtasks.node} and the dashboard. The engine's own leader jobs share the
 * {@code housekeeping} lease.
 *
 * <p>A job that throws is logged and runs again after its interval; the engine's health shows
 * when each job last ran and last failed. Leader jobs must be safe to run on two nodes at once:
 * for at most one heartbeat a hung leader may not know yet that it lost the lease. A job that
 * loops over batches should stop when {@link ExtensionContext.Job#runsHere} turns false.
 */
public interface HousekeepingJob {

    /** Where a job runs. */
    enum Scope {
        /** On every node that runs, such as a sweep of what this node holds. */
        EVERY_NODE,
        /** On one node at a time, the one holding the job's lease: cluster-wide work, done once. */
        LEADER
    }

    @FunctionalInterface
    interface Action {
        void run() throws Exception;
    }

    /** For the logs and health, such as "promotion"; with its extension's name, a leader job's lease. Unique per engine. */
    String name();

    Scope scope();

    /** The delay between the end of one run and the start of the next. */
    Duration interval();

    void run() throws Exception;

    /** Runs on one node at a time among those that have a job by this name; see the class comment. */
    static HousekeepingJob leader(String name, Duration interval, Action action) {
        return of(name, Scope.LEADER, interval, action);
    }

    /** Runs on every node that has it. */
    static HousekeepingJob everyNode(String name, Duration interval, Action action) {
        return of(name, Scope.EVERY_NODE, interval, action);
    }

    private static HousekeepingJob of(String name, Scope scope, Duration interval, Action action) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(action, "action");
        if (name.isBlank()) {
            throw new IllegalArgumentException("A job needs a name");
        }
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
