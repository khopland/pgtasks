package io.github.khopland.pgtasks.test;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresExtensionTest {

    @RegisterExtension
    static PostgresExtension postgres = PostgresExtension.migrated(database -> database.execute("CREATE TABLE marker (id int)"));

    @Test
    void runs_a_real_postgres(PostgresDatabase database) {
        // The major version is whatever the run asked for (pgtasks.test.postgres.version), checked when the server starts.
        assertThat((String) database.rows("SELECT version()").getFirst().getFirst()).startsWith("PostgreSQL "
            + System.getProperty("pgtasks.test.postgres.version", ""));
        assertThat(database.count("SELECT count(*) FROM marker")).isZero();
    }

    @Test
    void gives_each_test_its_own_database(PostgresDatabase database) {
        database.execute("INSERT INTO marker VALUES (1)");
        assertThat(database.count("SELECT count(*) FROM marker")).isEqualTo(1);
    }
}
