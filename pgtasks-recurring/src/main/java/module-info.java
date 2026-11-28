/** Recurring tasks: cron schedules that push a task once per run, built on the engine's extension hooks. */
module io.github.khopland.pgtasks.recurring {
    requires transitive io.github.khopland.pgtasks.engine;
    requires org.jspecify;
    requires org.slf4j;

    exports io.github.khopland.pgtasks.recurring;

    provides io.github.khopland.pgtasks.spi.StoreCapability
        with io.github.khopland.pgtasks.recurring.InMemoryScheduleCapability;
}
