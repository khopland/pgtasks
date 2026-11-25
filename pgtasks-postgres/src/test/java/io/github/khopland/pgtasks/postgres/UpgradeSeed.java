package io.github.khopland.pgtasks.postgres;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.List;
import java.util.Map;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.spi.TaskStore.NewTask;
import io.github.khopland.pgtasks.spi.TaskStore.Resolution;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

/**
 * Tasks in every state a release can leave behind, for {@link SchemaUpgradeTest}. Each task's
 * identifier names the state it is left in. Nothing here depends on when it is loaded: the
 * scheduled task waits a century, and everything else is due, finished or claimed by a node
 * whose heartbeat stops with the seed.
 *
 * <p>{@link SchemaSnapshot} runs this with a release's own code and dumps the rows, so a later
 * schema is tested against what that release wrote, not against what today's code would write.
 */
final class UpgradeSeed {

    static final String TOPIC = "upgrade";
    static final String NODE = "old-node";
    static final String KEY = "keyed-dedup";
    static final Map<String, String> HEADERS = Map.of("tenant", "north", "traceparent", "00-abc-def-01");

    /** Unfinished tasks the new schema must still run, by identifier. */
    static final List<String> UNFINISHED = List.of("ready", "keyed", "suspended", "dead", "retrying", "active", "superseding");

    private UpgradeSeed() {
    }

    static void seed(StoreFixture fixture) throws Exception {
        int topic = fixture.topic(TOPIC);
        int node = fixture.node(NODE);
        fixture.inTransaction(transaction -> {
            fixture.store().setRetention(transaction, topic, Map.of(Outcome.SUCCEEDED, Period.ofDays(30)));
            return null;
        });

        // One at a time through a claim, so each claim takes exactly the task it finishes.
        finish(fixture, topic, node, "succeeded", StoreFixture.succeeded());
        finish(fixture, topic, node, "filtered", StoreFixture.filtered("not for us"));
        finish(fixture, topic, node, "dead", StoreFixture.deadLetter());
        finish(fixture, topic, node, "retrying", StoreFixture.retrying(Duration.ofMillis(1)));
        // Claimed and never completed: its node stops heartbeating, so a new node requeues it.
        fixture.push(topic, "active");
        fixture.claim(topic, node);

        fixture.push(topic, Insertion.APPEND, StoreFixture.task("superseding"));
        fixture.push(topic, Insertion.SUPERSEDE, StoreFixture.task("superseding"));
        fixture.push(topic, "ready");
        fixture.push(topic, Insertion.APPEND, new NewTask("keyed", "{\"order\":7}", "com.example.Order", "ref-7", null, false, 5,
            KEY, null, null, HEADERS));
        fixture.push(topic, Insertion.APPEND, StoreFixture.suspended("suspended"));
        fixture.push(topic, Insertion.APPEND, StoreFixture.at("scheduled", Instant.now().plus(Duration.ofDays(36_500))));

        fixture.inTransaction(transaction -> {
            fixture.store().snapshotCounts(transaction);
            fixture.store().lead(transaction, "housekeeping", node, Duration.ofSeconds(30));
            return null;
        });
    }

    private static void finish(StoreFixture fixture, int topic, int node, String identifier, Resolution resolution) throws Exception {
        fixture.push(topic, identifier);
        List<ClaimedTask> claimed = fixture.claim(topic, node);
        if (claimed.size() != 1 || !claimed.getFirst().identifier().equals(identifier)) {
            throw new IllegalStateException("Expected to claim only " + identifier + ", got " + claimed);
        }
        fixture.complete(topic, node, claimed.getFirst(), resolution);
    }
}
