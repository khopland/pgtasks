/**
 * The Postgres store, its schema and the 1.x importer. The schema's SQL files are open, so
 * Flyway and Liquibase can read them from {@code io/github/khopland/pgtasks/postgres/db}.
 */
module io.github.khopland.pgtasks.postgres {
    requires transitive io.github.khopland.pgtasks.spi;
    requires org.postgresql.jdbc;
    requires org.slf4j;

    exports io.github.khopland.pgtasks.postgres;
    opens io.github.khopland.pgtasks.postgres.db;

    provides io.github.khopland.pgtasks.spi.StoreProvider with io.github.khopland.pgtasks.postgres.PostgresStoreProvider;
}
