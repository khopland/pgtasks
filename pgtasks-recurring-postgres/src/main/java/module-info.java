/** Shared schedules for pgtasks-recurring on Postgres. */
module io.github.khopland.pgtasks.recurring.postgres {
    requires transitive io.github.khopland.pgtasks.recurring;
    requires transitive io.github.khopland.pgtasks.postgres;
    requires io.github.khopland.pgtasks.spi;
    requires java.sql;
    requires org.jspecify;

    exports io.github.khopland.pgtasks.recurring.postgres;
    opens io.github.khopland.pgtasks.recurring.postgres.db;

    provides io.github.khopland.pgtasks.spi.StoreCapability
        with io.github.khopland.pgtasks.recurring.postgres.PostgresScheduleCapability;
}
