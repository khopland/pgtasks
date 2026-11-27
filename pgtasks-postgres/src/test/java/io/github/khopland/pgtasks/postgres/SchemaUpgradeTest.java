package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import io.github.khopland.pgtasks.spi.TaskStore.NewTask;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Upgrading from every release to this build's schema keeps every task usable. Each release in
 * {@link SchemaReleases} is applied with its own files, loaded with the rows that release's code
 * left in every state ({@link UpgradeSeed}), migrated with the current files, and then run by the
 * current store: due tasks run, the orphaned claim is requeued, the dead letter revives, the
 * suspended task releases, history is still there, and the dedup key still holds.
 *
 * <p>Until the first release there is no snapshot, so the test takes one of the current schema in
 * a scratch schema and upgrades from that: the same dump, load and checks, with nothing to
 * migrate. After a release, {@link SchemaSnapshot} adds it (RELEASING.md).
 */
class SchemaUpgradeTest {

    static final String UNRELEASED = "unreleased";

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.empty();

    static Stream<String> releases() {
        List<String> versions = SchemaReleases.versions();
        return versions.isEmpty() ? Stream.of(UNRELEASED) : versions.stream();
    }

    @ParameterizedTest(name = "from {0}")
    @MethodSource("releases")
    void tasks_in_every_state_survive_the_upgrade(String release, PostgresDatabase database) throws Exception {
        List<SchemaScript> scripts;
        String seed;
        if (release.equals(UNRELEASED)) {
            scripts = TaskSchema.scripts();
            seed = seedFromCurrentCode(database);
        } else {
            scripts = SchemaReleases.scripts(release);
            seed = SchemaReleases.seed(release);
        }
        try (Connection connection = database.connect()) {
            new SchemaMigrator(scripts, TaskSchema.DEFAULT_SCHEMA).migrate(connection);
        }
        SchemaReleases.load(database, seed, TaskSchema.DEFAULT_SCHEMA);

        TaskSchema.Status status = TaskSchema.migrate(database.dataSource());
        assertThat(status.version()).isEqualTo(TaskSchema.VERSION);

        try (PostgresStoreFixture fixture = new PostgresStoreFixture(database)) {
            fixture.store().prepare();
            check(fixture);
        }
    }

    /** What the seed left behind still works under the current schema and store. */
    private static void check(PostgresStoreFixture fixture) throws Exception {
        int topic = fixture.inTransaction(transaction -> fixture.store().findTopic(transaction, UpgradeSeed.TOPIC)).orElseThrow();
        assertThat(state(fixture, topic, "succeeded")).isEqualTo("succeeded");
        assertThat(state(fixture, topic, "filtered")).isEqualTo("filtered");
        assertThat(states(fixture, topic, "superseding")).containsExactly("redundant", "ready");
        assertThat(state(fixture, topic, "scheduled")).isEqualTo("scheduled");
        assertThat(state(fixture, topic, "dead")).isEqualTo("dead_letter");
        assertThat(state(fixture, topic, "suspended")).isEqualTo("suspended");
        assertThat(fixture.inTransaction(transaction -> fixture.store().retention(transaction)).get(topic)).isNotEmpty();

        // The dedup key still holds while its task is unfinished.
        NewTask again = new NewTask("keyed-again", null, null, null, null, false, 0, UpgradeSeed.KEY, null, null, Map.of());
        assertThat(fixture.push(topic, Insertion.APPEND, again).getFirst().status()).isEqualTo(Pushed.Status.DUPLICATE);

        // A new node takes over: the old node's claim is requeued, the operator revives and releases.
        // An hour on: the old node's heartbeat is stale and the retry is due. The new node registers after.
        fixture.advance(Duration.ofHours(1));
        int node = fixture.node("new-node");
        fixture.requeueExpired(node, Duration.ofMinutes(1));
        fixture.promoteDue();
        long dead = id(fixture, topic, "dead");
        fixture.inTransaction(transaction -> {
            fixture.store().revive(transaction, topic, List.of(dead), 10);
            fixture.store().releaseSuspended(transaction, topic, TaskQuery.ALL.withIdentifier("suspended"), 10);
            return null;
        });

        List<ClaimedTask> claimed = fixture.claim(topic, node);
        assertThat(claimed).extracting(ClaimedTask::identifier).containsExactlyInAnyOrderElementsOf(UpgradeSeed.UNFINISHED);
        ClaimedTask keyed = claimed.stream().filter(task -> task.identifier().equals("keyed")).findFirst().orElseThrow();
        assertThat(claimed.getFirst()).as("the highest priority first").isEqualTo(keyed);
        assertThat(keyed.headers()).isEqualTo(UpgradeSeed.HEADERS);
        assertThat(keyed.payload()).isEqualTo("{\"order\":7}");
        assertThat(keyed.reference()).isEqualTo("ref-7");
        for (ClaimedTask task : claimed) {
            fixture.complete(topic, node, task, StoreFixture.succeeded());
        }

        assertThat(fixture.counts(topic).scheduled()).isEqualTo(1);
        assertThat(fixture.claim(topic, node)).isEmpty();
        // New tasks get new ids after the seed's.
        long newest = fixture.push(topic, "after-upgrade");
        assertThat(newest).isGreaterThan(id(fixture, topic, "scheduled"));
    }

    /** Before the first release: the rows the current code writes, dumped from a scratch schema. */
    private static String seedFromCurrentCode(PostgresDatabase database) throws Exception {
        String scratch = "pgtasks_seed";
        TaskSchema.migrate(database.dataSource(), scratch);
        try (PostgresStoreFixture fixture = new PostgresStoreFixture(database, database.dataSource(), scratch)) {
            UpgradeSeed.seed(fixture);
        }
        String seed = SchemaReleases.dump(database, scratch, "");
        database.execute("DROP SCHEMA " + scratch + " CASCADE");
        return seed;
    }

    private static List<TaskView> views(PostgresStoreFixture fixture, int topic, String identifier) throws Exception {
        List<Long> ids = fixture.database.rows("SELECT id FROM pgtasks.task WHERE topic_id = ? AND identifier = ? ORDER BY id", topic, identifier)
            .stream().map(row -> ((Number) row.getFirst()).longValue()).toList();
        assertThat(ids).as("tasks named " + identifier).isNotEmpty();
        return ids.stream().map(id -> {
            try {
                return fixture.view(id).orElseThrow();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    private static List<String> states(PostgresStoreFixture fixture, int topic, String identifier) throws Exception {
        return views(fixture, topic, identifier).stream().map(TaskView::state).toList();
    }

    private static String state(PostgresStoreFixture fixture, int topic, String identifier) throws Exception {
        return views(fixture, topic, identifier).getFirst().state();
    }

    private static long id(PostgresStoreFixture fixture, int topic, String identifier) throws Exception {
        return views(fixture, topic, identifier).getFirst().id();
    }
}
