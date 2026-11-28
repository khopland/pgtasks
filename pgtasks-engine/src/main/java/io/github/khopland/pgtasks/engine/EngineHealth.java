package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.engine.extension.HousekeepingJob;

import org.jspecify.annotations.Nullable;

/**
 * How one node is doing, for health checks; from {@link TaskEngine#health()}. The engine makes
 * these, and later versions may add to them.
 */
public final class EngineHealth {

    private final boolean running;
    private final String node;
    private final int nodeId;
    private final @Nullable Instant lastHeartbeat;
    private final @Nullable Duration heartbeatAge;
    private final Duration leaseTerm;
    private final boolean leader;
    private final Notifications notifications;
    private final List<StuckClaim> stuck;
    private final List<HeldTopic> held;
    private final List<JobStatus> jobs;

    EngineHealth(boolean running, String node, int nodeId, @Nullable Instant lastHeartbeat, @Nullable Duration heartbeatAge, Duration leaseTerm,
                 boolean leader, Notifications notifications, List<StuckClaim> stuck, List<HeldTopic> held, List<JobStatus> jobs) {
        this.running = running;
        this.node = Objects.requireNonNull(node, "node");
        this.nodeId = nodeId;
        this.lastHeartbeat = lastHeartbeat;
        this.heartbeatAge = heartbeatAge;
        this.leaseTerm = Objects.requireNonNull(leaseTerm, "leaseTerm");
        this.leader = leader;
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.stuck = List.copyOf(stuck);
        this.held = List.copyOf(held);
        this.jobs = List.copyOf(jobs);
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

    /** Whether the engine is started and not stopped. */
    public boolean running() {
        return running;
    }

    /** This node's name. */
    public String node() {
        return node;
    }

    /** This node's id in pgtasks.node; -1 if not started. */
    public int nodeId() {
        return nodeId;
    }

    /** When this node last recorded its heartbeat, or null if not started. */
    public @Nullable Instant lastHeartbeat() {
        return lastHeartbeat;
    }

    /** How long ago that was. */
    public @Nullable Duration heartbeatAge() {
        return heartbeatAge;
    }

    /**
     * After how long without a heartbeat the node loses the housekeeping lease; other nodes
     * requeue its tasks after the longer expiry.
     */
    public Duration leaseTerm() {
        return leaseTerm;
    }

    /** Whether this node runs the cluster-wide housekeeping. */
    public boolean leader() {
        return leader;
    }

    /** Whether wake-ups are getting through. */
    public Notifications notifications() {
        return notifications;
    }

    /**
     * Claims of this node that have run past their topic's batch deadline by more than a
     * heartbeat: a handler that ignores cancellation, or a completion that can't reach the database.
     */
    public List<StuckClaim> stuck() {
        return stuck;
    }

    /** Topics an extension's claim gate holds back on this node now; see {@link TaskEngine#heldTopics()}. */
    public List<HeldTopic> held() {
        return held;
    }

    /** The jobs this node runs, the engine's and the extensions', with when each last ran and last failed. */
    public List<JobStatus> jobs() {
        return jobs;
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

    @Override
    public String toString() {
        return "EngineHealth[" + node + " (" + nodeId + "), " + (running ? "running" : "not running") + ", heartbeat " + heartbeatAge
            + (leader ? ", leader" : "") + ", notifications " + notifications + ", stuck " + stuck + ", held " + held + "]";
    }

    /** A topic a claim gate holds back on this node. */
    public static final class HeldTopic {

        private final String topic;
        private final String gate;
        private final @Nullable String reason;
        private final Instant since;
        private final Instant until;

        HeldTopic(String topic, String gate, @Nullable String reason, Instant since, Instant until) {
            this.topic = Objects.requireNonNull(topic, "topic");
            this.gate = Objects.requireNonNull(gate, "gate");
            this.reason = reason;
            this.since = Objects.requireNonNull(since, "since");
            this.until = Objects.requireNonNull(until, "until");
        }

        public String topic() {
            return topic;
        }

        /** The name of the extension whose gate held the last claim back. */
        public String gate() {
            return gate;
        }

        /** Why, as the gate said with its wait, such as "paused"; null if it didn't say. */
        public @Nullable String reason() {
            return reason;
        }

        /** When its claims were first held back, without a claim in between. */
        public Instant since() {
            return since;
        }

        /**
         * Until when the gate said to wait the last time it was asked, by the store's clock; the
         * topic asks it again at least once a poll interval meanwhile, and at once when it is woken.
         */
        public Instant until() {
            return until;
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return other instanceof HeldTopic that && that.topic.equals(topic) && that.gate.equals(gate) && Objects.equals(that.reason, reason)
                && that.since.equals(since) && that.until.equals(until);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, gate, reason, since, until);
        }

        @Override
        public String toString() {
            return "HeldTopic[" + topic + " by " + gate + (reason == null ? "" : " (" + reason + ")") + " since " + since + " until " + until + "]";
        }
    }

    /** One job, and how its runs went on this node. */
    public static final class JobStatus {

        private final String name;
        private final HousekeepingJob.Scope scope;
        private final boolean runsHere;
        private final @Nullable Instant lastRun;
        private final @Nullable Instant lastFailure;
        private final @Nullable String lastError;
        private final int failuresInARow;

        JobStatus(String name, HousekeepingJob.Scope scope, boolean runsHere, @Nullable Instant lastRun, @Nullable Instant lastFailure,
                  @Nullable String lastError, int failuresInARow) {
            this.name = Objects.requireNonNull(name, "name");
            this.scope = Objects.requireNonNull(scope, "scope");
            this.runsHere = runsHere;
            this.lastRun = lastRun;
            this.lastFailure = lastFailure;
            this.lastError = lastError;
            this.failuresInARow = failuresInARow;
        }

        public String name() {
            return name;
        }

        public HousekeepingJob.Scope scope() {
            return scope;
        }

        /** Whether it runs on this node now: always for an every-node job, and for a leader job while this node holds its lease. */
        public boolean runsHere() {
            return runsHere;
        }

        /** When its last run on this node ended, failed or not; null if it hasn't run here. */
        public @Nullable Instant lastRun() {
            return lastRun;
        }

        /** When its last failed run ended, or null. */
        public @Nullable Instant lastFailure() {
            return lastFailure;
        }

        /** What that run failed with. */
        public @Nullable String lastError() {
            return lastError;
        }

        /** The runs that failed since the last one that didn't; zero once one succeeds. */
        public int failuresInARow() {
            return failuresInARow;
        }

        /** Whether its last run here failed. */
        public boolean failing() {
            return failuresInARow > 0;
        }

        @Override
        public String toString() {
            return "JobStatus[" + name + ", " + scope + (runsHere ? ", runs here" : "") + ", last run " + lastRun
                + (failing() ? ", failing " + failuresInARow + " in a row: " + lastError : "") + "]";
        }
    }
}
