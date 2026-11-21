package io.github.khopland.pgtasks.postgres;

import io.github.khopland.pgtasks.postgres.PostgresStoreFixture.QueueStatistics;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.PostgresExtension;
import io.github.khopland.pgtasks.test.tck.JunctionTck;
import io.github.khopland.pgtasks.test.tck.StoreFixture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@ParameterizedClass(name = "queue statistics: {0}")
@EnumSource(QueueStatistics.class)
class PostgresJunctionTest extends JunctionTck {

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
