package com.example.orders;

import java.time.Duration;
import java.util.Map;

import io.github.khopland.pgtasks.engine.Rate;
import io.github.khopland.pgtasks.engine.TaskGlobals;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Limits and node settings shared by every module, and how the pgtasks schema gets migrated. */
@Configuration
class PgtasksConfig {

    @Bean
    TaskGlobals globals(@Value("${orders.heartbeat:5s}") Duration heartbeat, @Value("${orders.heartbeat-expiry:2m}") Duration expiry) {
        return globals -> globals
            .heartbeat(heartbeat, expiry)
            .limit("gateway", limit -> limit.rate(Rate.perSecond(150)).burst(30))
            .limit("carrier", limit -> limit.rate(Rate.perSecond(150)).burst(30))
            .limit("mailer", limit -> limit.rate(Rate.perSecond(300)).burst(50));
    }

    /**
     * The team runs every schema change through Flyway as the owner role, and the application runs
     * as a role without DDL rights (pgtasks.schema-mode: validate). The pgtasks files get a Flyway
     * instance of their own, as docs/operations.md says, applied before the application's
     * migrations, which grant the runtime role access to the pgtasks schema.
     */
    @Bean
    FlywayMigrationStrategy migrations(@Value("${pgtasks.schema:pgtasks}") String schema) {
        return application -> {
            Flyway.configure()
                .dataSource(application.getConfiguration().getDataSource())
                .locations("classpath:io/github/khopland/pgtasks/postgres/db")
                .schemas(schema)
                .placeholders(Map.of("pgtasks_schema", schema))
                .table("flyway_pgtasks_history")
                .load()
                .migrate();
            application.migrate();
        };
    }
}
