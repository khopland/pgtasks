package com.example.orders;

import java.util.Map;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** How the pgtasks schema gets migrated. The heartbeat is in application.yml (pgtasks.heartbeat, pgtasks.node-expiry). */
@Configuration
class PgtasksConfig {

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
