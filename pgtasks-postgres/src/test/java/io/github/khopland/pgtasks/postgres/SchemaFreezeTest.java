package io.github.khopland.pgtasks.postgres;

import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Released versioned files never change: a database that applied one would no longer match it, and
 * the runner, Flyway and Liquibase all refuse to start then. Change the schema with the next
 * versioned file instead (docs/operations.md, "Changing the schema"), and add its checksum here
 * when it is released.
 */
class SchemaFreezeTest {

    /** SHA-256 of each released versioned file, line endings normalized, placeholder unfilled: what the runner records. */
    static final Map<String, String> RELEASED = Map.of(
        // 0.1.0
        "V001__tasks_schema.sql", "02bb99240109e5a8fcdc2e88838d1d0061dd803a0568be8eba558ac1bff5ed56"
    );

    @Test
    void released_versioned_files_are_unchanged() {
        Map<String, String> shipped = new TreeMap<>();
        TaskSchema.scripts().stream().filter(script -> !script.repeatable()).forEach(script -> shipped.put(script.name(), script.checksum()));

        RELEASED.forEach((name, checksum) -> assertThat(shipped)
            .as(name + " was released, so it must never change or go away; put the change in a new versioned file")
            .containsEntry(name, checksum));
    }

    @Test
    void the_released_files_kept_for_the_upgrade_test_are_the_frozen_ones() {
        for (SchemaScript script : SchemaUpgradeTest.released("0.1.0")) {
            if (!script.repeatable()) {
                assertThat(script.checksum()).as(script.name() + " as kept in the test resources").isEqualTo(RELEASED.get(script.name()));
            }
        }
    }
}
