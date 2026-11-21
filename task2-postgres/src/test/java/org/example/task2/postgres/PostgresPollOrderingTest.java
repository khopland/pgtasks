package org.example.task2.postgres;

import org.example.task2.postgres.PostgresStoreFixture.QueueStatistics;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.example.task2.test.tck.PollOrderingTck;
import org.example.task2.test.tck.StoreFixture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@ParameterizedClass(name = "queue statistics: {0}")
@EnumSource(QueueStatistics.class)
class PostgresPollOrderingTest extends PollOrderingTck {

    @Parameter
    QueueStatistics statistics;

    @RegisterExtension
    static PostgresExtension postgres = PostgresStoreFixture.extension();

    private PostgresDatabase database;

    @BeforeEach
    void database(PostgresDatabase database) {
        this.database = database;
    }

    @Override
    protected StoreFixture createFixture() {
        return PostgresStoreFixture.create(database, statistics);
    }
}
