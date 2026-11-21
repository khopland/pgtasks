package io.github.khopland.pgtasks.postgres;

import java.time.Duration;

import io.github.khopland.pgtasks.spi.TaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

/** The TCK fixture for Postgres. Time moves by backdating the timestamps the store compares against now(). */
class PostgresStoreFixture extends StoreFixture {

    static PostgresExtension extension() {
        return PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));
    }

    /**
     * The queue's statistics when a TCK test starts. Every Postgres TCK class runs once with each,
     * so every statement is also tested while the planner believes the queue is empty.
     */
    enum QueueStatistics {
        /** A new database: nothing analyzed yet. */
        FRESH,
        /** Analyzed right after a burst drained, with ballast it doesn't know about; see {@link DrainedQueue}. */
        DRAINED
    }

    static PostgresStoreFixture create(PostgresDatabase database, QueueStatistics statistics) {
        return switch (statistics) {
            case FRESH -> new PostgresStoreFixture(database);
            case DRAINED -> {
                DrainedQueue drained = DrainedQueue.create();
                yield new PostgresStoreFixture(drained.database(), drained.dataSource()) {
                    @Override
                    public void close() {
                        try {
                            drained.check();
                        } finally {
                            drained.close();
                        }
                    }
                };
            }
        };
    }

    final PostgresDatabase database;
    private final PostgresTaskStore store;

    PostgresStoreFixture(PostgresDatabase database) {
        this(database, database.dataSource());
    }

    private PostgresStoreFixture(PostgresDatabase database, javax.sql.DataSource dataSource) {
        this.database = database;
        this.store = new PostgresTaskStore(dataSource);
    }

    @Override
    public TaskStore store() {
        return store;
    }

    @Override
    public void advance(Duration duration) {
        String interval = "interval '" + duration.toMillis() + " milliseconds'";
        database.execute("UPDATE tasks.queue SET run_at = run_at - " + interval + " WHERE run_at IS NOT NULL");
        database.execute("UPDATE tasks.node SET heartbeat_at = heartbeat_at - " + interval);
        database.execute("UPDATE tasks.rate_admission SET refilled_at = refilled_at - " + interval);
        database.execute("UPDATE tasks.rate_limit SET until = until - " + interval + " WHERE until IS NOT NULL");
        database.execute("UPDATE tasks.lease SET acquired_at = acquired_at - " + interval + ", expires_at = expires_at - " + interval);
        database.execute("UPDATE tasks.topic SET revive_swept_at = revive_swept_at - " + interval + " WHERE revive_swept_at IS NOT NULL");
        database.execute("UPDATE tasks.schedule SET next_run_at = next_run_at - " + interval);
    }
}
