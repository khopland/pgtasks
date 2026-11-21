package io.github.khopland.pgtasks.spring;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import io.github.khopland.pgtasks.engine.EngineHealth;
import io.github.khopland.pgtasks.engine.TaskEngine;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;

/**
 * Health of this node's engine, as {@code tasks} in {@code /actuator/health}.
 *
 * <ul>
 *   <li>UP while the engine runs and its heartbeat is on time.</li>
 *   <li>DOWN if the heartbeat is older than the lease term (four heartbeats): the node has lost
 *       the housekeeping lease, and after the expiry other nodes requeue its tasks. Also DOWN if
 *       the database can't be reached.</li>
 *   <li>OUT_OF_SERVICE before start and after stop.</li>
 *   <li>UP on a node that only pushes ({@code pgtasks.enabled=false}): there is nothing of its own to
 *       be unhealthy about, and the database's own indicator covers its pushes.</li>
 * </ul>
 *
 * Disconnected notifications and claims stuck past their deadline are in the details, without
 * changing the status: the engine still works without notifications, only later, and a stuck
 * handler holds one batch while the rest of the node goes on.
 */
public class TaskHealthIndicator extends AbstractHealthIndicator {

    private static final int STUCK_SHOWN = 10;

    private final TaskEngine engine;
    private final boolean processing;

    public TaskHealthIndicator(TaskEngine engine) {
        this(engine, true);
    }

    /** @param processing false on a node that only pushes, which reports UP */
    public TaskHealthIndicator(TaskEngine engine, boolean processing) {
        super("The task engine's health check failed");
        this.engine = engine;
        this.processing = processing;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        if (!processing) {
            builder.up().withDetail("processing", "disabled on this node (pgtasks.enabled=false); it only pushes");
            return;
        }
        EngineHealth health = engine.health();
        if (!health.running()) {
            builder.outOfService().withDetail("node", health.node());
            return;
        }
        if (health.heartbeatOnTime()) {
            builder.up();
        } else {
            builder.down();
        }
        builder.withDetail("node", health.node())
            .withDetail("nodeId", health.nodeId())
            .withDetail("leader", health.leader())
            .withDetail("lastHeartbeat", String.valueOf(health.lastHeartbeat()))
            .withDetail("heartbeatAge", String.valueOf(health.heartbeatAge()))
            .withDetail("notifications", health.notifications().name().toLowerCase(Locale.ROOT))
            .withDetail("stuckClaims", health.stuck().size())
            .withDetail("openBreakers", health.openBreakers());
        if (!health.stuck().isEmpty()) {
            builder.withDetail("stuck", health.stuck().stream().limit(STUCK_SHOWN).map(stuck -> {
                Map<String, Object> claim = new LinkedHashMap<>();
                claim.put("topic", stuck.topic());
                claim.put("task", stuck.taskId());
                claim.put("attempt", stuck.attempt());
                claim.put("running", stuck.running().toString());
                return claim;
            }).toList());
        }
    }
}
