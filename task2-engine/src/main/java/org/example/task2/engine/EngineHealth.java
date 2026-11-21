package org.example.task2.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * How one node is doing, for health checks.
 *
 * @param running        whether the engine is started and not stopped
 * @param node           this node's name, and its id in tasks.node (-1 if not started)
 * @param lastHeartbeat  when this node last recorded its heartbeat, or null if not started
 * @param heartbeatAge   how long ago that was
 * @param leaseTerm      after how long without a heartbeat the node loses the housekeeping lease;
 *                       other nodes requeue its tasks after the longer expiry
 * @param leader         whether this node runs the cluster-wide housekeeping
 * @param notifications  whether wake-ups are getting through
 * @param stuck          claims of this node that have run past their topic's batch deadline by
 *                       more than a heartbeat: a handler that ignores cancellation, or a
 *                       completion that can't reach the database
 * @param openBreakers   topics this node has stopped claiming from, but for probes, because most
 *                       of their recent tasks failed
 */
public record EngineHealth(
    boolean running,
    String node,
    int nodeId,
    Instant lastHeartbeat,
    Duration heartbeatAge,
    Duration leaseTerm,
    boolean leader,
    Notifications notifications,
    List<StuckClaim> stuck,
    List<String> openBreakers
) {

    public EngineHealth {
        stuck = List.copyOf(stuck);
        openBreakers = List.copyOf(openBreakers);
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

    /** Whether the heartbeat is on time: no older than the lease term. */
    public boolean heartbeatOnTime() {
        return heartbeatAge != null && heartbeatAge.compareTo(leaseTerm) <= 0;
    }

    /** Running, with its heartbeat on time. Notifications and stuck claims are reported but don't count. */
    public boolean healthy() {
        return running && heartbeatOnTime();
    }
}
