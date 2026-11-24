/**
 * The pgtasks schema as SQL files, in the order of
 * {@link io.github.khopland.pgtasks.postgres.TaskSchema#FILES}: versioned {@code V*.sql} files and
 * repeatable {@code R__*.sql} ones, with {@code ${pgtasks_schema}} for the schema's name, and a
 * Liquibase {@code changelog.xml}. The built-in runner applies them; Flyway and Liquibase read
 * them from {@code classpath:io/github/khopland/pgtasks/postgres/db}, which this module opens.
 */
package io.github.khopland.pgtasks.postgres.db;
