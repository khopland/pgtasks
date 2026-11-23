package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.NodeView;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-23: a node the cluster forgot (silent for days without claims, as after a
 * suspended VM) failed every heartbeat and, on Postgres, every claim on the node foreign key,
 * for good. It registers again under a new id instead.
 */
class ForgottenNodeTest {

    @RegisterExtension
    static PostgresExtension postgres = EngineFixture.extension();

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private final MemoryEngines engines = new MemoryEngines();
    private final List<String> ran = new CopyOnWriteArrayList<>();
    private EngineFixture fixture;

    @AfterEach
    void tearDown() {
        engines.close();
        if (fixture != null) {
            fixture.close();
        }
    }

    private List<Topic<?>> topic() {
        return List.of(Topic.of(WORK, batch -> batch.decide(task -> {
            ran.add(task.identifier());
            return Decision.success();
        })));
    }

    @Test
    void a_forgotten_node_registers_again_and_goes_on_working() throws Exception {
        // Heartbeats only when the test asks, so the node can be silent for days.
        TaskEngine engine = engines.start("node-a", topic(), settings -> settings.withHeartbeat(Duration.ofHours(1), Duration.ofHours(2)));
        int forgotten = engine.nodeId();
        // The scheduled first heartbeat, the only one for an hour, takes the lease.
        EngineFixture.await("the first heartbeat", Duration.ofSeconds(5), engine::leadsHousekeeping);
        engines.clock.advance(Housekeeper.FORGET_NODES_AFTER.plusHours(1));
        int deleted = EngineCore.transact(engines.store, transaction -> engines.store.deleteDeadNodes(transaction, Housekeeper.FORGET_NODES_AFTER));
        assertThat(deleted).isEqualTo(1);

        engine.heartbeatNow();

        assertThat(engine.nodeId()).isNotEqualTo(forgotten);
        assertThat(engine.admin().nodes()).extracting(NodeView::id).containsExactly(engine.nodeId());
        assertThat(engine.leadsHousekeeping()).as("took the lease under its new id").isTrue();
        assertThat(engine.health().nodeId()).isEqualTo(engine.nodeId());

        engine.tasks().independently().push(WORK, "after", "payload");
        TaskTester.of(engine).drain(WORK);
        assertThat(ran).containsExactly("after");
    }

    @Test
    void on_postgres_a_forgotten_node_claims_again_once_it_registered_again(PostgresDatabase database) throws Exception {
        fixture = new EngineFixture(database);
        TaskEngine engine = fixture.start("node-a", topic());
        int forgotten = engine.nodeId();

        // As deleteDeadNodes does after three silent days; the lease goes with the row.
        database.execute("DELETE FROM pgtasks.node WHERE id = " + forgotten);
        EngineFixture.await("registered again", Duration.ofSeconds(5), () -> engine.nodeId() != forgotten);
        engine.tasks().independently().push(WORK, "after", "payload");

        EngineFixture.await("the task ran", Duration.ofSeconds(5), () -> !ran.isEmpty());
        assertThat(ran).containsExactly("after");
        assertThat(database.count("SELECT count(*) FROM pgtasks.node WHERE id = ?", forgotten)).isZero();
        assertThat(database.count("SELECT count(*) FROM pgtasks.node WHERE id = ?", engine.nodeId())).isEqualTo(1);
        EngineFixture.await("leading under its new id", Duration.ofSeconds(5), engine::leadsHousekeeping);
    }
}
