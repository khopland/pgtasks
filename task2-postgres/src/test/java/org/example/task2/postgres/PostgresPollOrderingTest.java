package org.example.task2.postgres;

import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.example.task2.test.tck.PollOrderingTck;
import org.example.task2.test.tck.StoreFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

class PostgresPollOrderingTest extends PollOrderingTck {

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    private PostgresDatabase database;

    @BeforeEach
    void database(PostgresDatabase database) {
        this.database = database;
    }

    @Override
    protected StoreFixture createFixture() {
        return new PostgresStoreFixture(database);
    }
}
