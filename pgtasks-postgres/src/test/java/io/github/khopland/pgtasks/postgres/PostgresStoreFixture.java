package io.github.khopland.pgtasks.postgres;

import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

/** The TCK fixture for Postgres. Time moves by backdating the timestamps the store compares against now(). */
class PostgresStoreFixture extends StoreFixture {

    static PostgresExtension extension() {
        return PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));
    }

    /** A schema name other than the default, for the drained pass: every statement must follow the configured name. */
    static final String CUSTOM_SCHEMA = "pgtasks_tck";

    /**
     * The queue's statistics when a TCK test starts. Every Postgres TCK class runs once with each,
     * so every statement is also tested while the planner believes the queue is empty, and while it
     * believes the task table is empty too.
     *
     * <p>The drained and emptied passes also run on a schema named {@value #CUSTOM_SCHEMA}, in a database without a
     * {@code pgtasks} schema, so the whole TCK and the plan audit run once under a configured name at no
     * extra cost. A plan doesn't depend on the schema's name.
     */
    enum QueueStatistics {
        /** A new database: nothing analyzed yet. The default schema. */
        FRESH(TaskSchema.DEFAULT_SCHEMA),
        /** Analyzed right after a burst drained, with ballast it doesn't know about; see {@link DrainedQueue}. */
        DRAINED(CUSTOM_SCHEMA),
        /**
         * The queue and the task table both analyzed while empty, then history and ballast they
         * don't know about: an analyze right after retention emptied the task table. The store is
         * prepared first, as an engine does, which repairs the task table's statistics; see
         * {@link DrainedQueue} and EmptiedTaskStatisticsTest.
         */
        EMPTIED(CUSTOM_SCHEMA);

        final String schema;

        QueueStatistics(String schema) {
            this.schema = schema;
        }

        @Override
        public String toString() {
            return name() + ", schema " + schema;
        }
    }

    static PostgresStoreFixture create(PostgresDatabase database, QueueStatistics statistics) {
        return switch (statistics) {
            case FRESH -> new PostgresStoreFixture(database);
            case DRAINED, EMPTIED -> {
                DrainedQueue drained = DrainedQueue.create(statistics.schema, statistics == QueueStatistics.EMPTIED);
                PostgresStoreFixture fixture = new PostgresStoreFixture(drained.database(), drained.dataSource(), drained.schema()) {
                    @Override
                    public void close() {
                        try {
                            drained.check();
                        } finally {
                            drained.close();
                        }
                    }
                };
                if (statistics == QueueStatistics.EMPTIED) {
                    // As an engine does before anything else: the store repairs the task table's statistics.
                    try {
                        fixture.store.prepare();
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }
                }
                yield fixture;
            }
        };
    }

    final PostgresDatabase database;
    final String schema;
    private final PostgresTaskStore store;

    PostgresStoreFixture(PostgresDatabase database) {
        this(database, database.dataSource(), TaskSchema.DEFAULT_SCHEMA);
    }

    PostgresStoreFixture(PostgresDatabase database, DataSource dataSource, String schema) {
        this.database = database;
        this.schema = schema;
        this.store = new PostgresTaskStore(dataSource, SchemaMode.MIGRATE, schema);
    }

    @Override
    public TaskStore store() {
        return store;
    }

    /** Runs SQL written for the default schema on this fixture's. */
    void execute(String sql) {
        database.execute(TaskSchema.qualify(sql, schema));
    }

    @Override
    public void advance(Duration duration) {
        String interval = "interval '" + duration.toMillis() + " milliseconds'";
        execute("UPDATE pgtasks.queue SET run_at = run_at - " + interval + " WHERE run_at IS NOT NULL");
        execute("UPDATE pgtasks.node SET heartbeat_at = heartbeat_at - " + interval);
        execute("UPDATE pgtasks.lease SET acquired_at = acquired_at - " + interval + ", expires_at = expires_at - " + interval);
    }
}
