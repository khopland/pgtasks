package org.example.task2.postgres;

import java.time.Duration;

import org.example.task2.spi.TaskStore;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.example.task2.test.tck.StoreFixture;

/** The TCK fixture for Postgres. Time moves by backdating the timestamps the store compares against now(). */
final class PostgresStoreFixture extends StoreFixture {

    static PostgresExtension extension() {
        return PostgresExtension.migrated(database -> TaskSchema.migrate(database.dataSource()));
    }

    final PostgresDatabase database;
    private final PostgresTaskStore store;

    PostgresStoreFixture(PostgresDatabase database) {
        this.database = database;
        this.store = new PostgresTaskStore(database.dataSource());
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
    }
}
