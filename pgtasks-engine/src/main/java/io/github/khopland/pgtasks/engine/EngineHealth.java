package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.engine.extension.HousekeepingJob;

import org.jspecify.annotations.Nullable;

/**
 * How one node is doing, for health checks.
 *
 * @param running        whether the engine is started and not stopped
 * @param node           this node's name, and its id in pgtasks.node (-1 if not started)
 * @param lastHeartbeat  when this node last recorded its heartbeat, or null if not started
 * @param heartbeatAge   how long ago that was
 * @param leaseTerm      after how long without a heartbeat the node loses the housekeeping lease;
 *                       other nodes requeue its tasks after the longer expiry
 * @param leader         whether this node runs the cluster-wide housekeeping
 * @param notifications  whether wake-ups are getting through
 * @param stuck          claims of this node that have run past their topic's batch deadline by
 *                       more than a heartbeat: a handler that ignores cancellation, or a
 *                       completion that can't reach the database
 * @param held           topics an extension's claim gate holds back on this node now, such as a
 *                       rate limit or a pause: they claim nothing until it lets them
 * @param jobs           the jobs this node runs, the engine's and the extensions', with when each
 *                       last ran and last failed
 */
public record EngineHealth(
    boolean running,
    String node,
    int nodeId,
    @Nullable Instant lastHeartbeat,
    @Nullable Duration heartbeatAge,
    Duration leaseTerm,
    boolean leader,
    Notifications notifications,
    List<StuckClaim> stuck,
    List<HeldTopic> held,
    List<JobStatus> jobs
) {

    public EngineHealth {
        stuck = List.copyOf(stuck);
        held = List.copyOf(held);
        jobs = List.copyOf(jobs);
    }

    public enum Notifications {
        /** None configured: the engine relies on polling. */
        NONE,
        CONNECTED,
        /** Reconnecting; polling still finds the work, only later. */
        DISCONNECTED
    }

    public record StuckClaim(String topic, long taskId, int attempt, Instant claimedAt, Duration running) {
    }

    /**
     * A topic a claim gate holds back.
     *
     * @param gate  the name of the extension whose gate held the last claim back
     * @param since when its claims were first held back, without a claim in between
     * @param until until when the gate said to wait the last time it was asked, by the store's
     *              clock; the topic asks it again at least once a poll interval meanwhile, and at
     *              once when it is woken
     */
    public record HeldTopic(String topic, String gate, Instant since, Instant until) {

        public HeldTopic {
            Objects.requireNonNull(topic, "topic");
            Objects.requireNonNull(gate, "gate");
            Objects.requireNonNull(since, "since");
            Objects.requireNonNull(until, "until");
        }
    }

    /**
     * One job, and how its runs went on this node.
     *
     * @param runsHere       whether it runs on this node now: always for an every-node job, and for
     *                       a leader job while this node holds its lease
     * @param lastRun        when its last run on this node ended, failed or not; null if it hasn't run here
     * @param lastFailure    when its last failed run ended, or null
     * @param lastError      what that run failed with
     * @param failuresInARow the runs that failed since the last one that didn't; zero once one succeeds
     */
    public record JobStatus(String name, HousekeepingJob.Scope scope, boolean runsHere, @Nullable Instant lastRun,
                            @Nullable Instant lastFailure, @Nullable String lastError, int failuresInARow) {

        public JobStatus {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(scope, "scope");
        }

        /** Whether its last run here failed. */
        public boolean failing() {
            return failuresInARow > 0;
        }
    }

    /** Whether the heartbeat is on time: no older than the lease term. */
    public boolean heartbeatOnTime() {
        return heartbeatAge != null && heartbeatAge.compareTo(leaseTerm) <= 0;
    }

    /**
     * Running, with its heartbeat on time. Notifications, stuck claims, held topics and failing
     * jobs are reported but don't count.
     */
    public boolean healthy() {
        return running && heartbeatOnTime();
    }
}
