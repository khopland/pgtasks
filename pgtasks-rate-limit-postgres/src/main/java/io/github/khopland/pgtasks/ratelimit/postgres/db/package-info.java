/**
 * The cluster rate limit's own SQL files, separate from the engine's. Applications that apply
 * schema files with Flyway, Liquibase or a DBA pipeline can use these from the jar, with
 * {@code ${pgtasks_schema}} set to the engine's schema.
 */
package io.github.khopland.pgtasks.ratelimit.postgres.db;
