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
 *
 * <p>Repeatable files may change, but each change must raise the {@code -- revision: <n>} in the
 * file's header, so the runner can tell a newer definition from an older one and an older node
 * never rolls a newer one back during a rolling deploy. Every revision's checksum is pinned here:
 * changing a file without raising its revision fails, and a new revision needs its checksum added.
 */
class SchemaFreezeTest {

    /** SHA-256 of each released versioned file, line endings normalized, placeholder unfilled: what the runner records. */
    static final Map<String, String> RELEASED = Map.of(
        // 0.1.0
        "V001__tasks_schema.sql", "02bb99240109e5a8fcdc2e88838d1d0061dd803a0568be8eba558ac1bff5ed56"
    );

    /** SHA-256 of each revision of each repeatable file, keyed {@code <name>@<revision>}. Add a line for every new revision. */
    static final Map<String, String> REVISIONS = Map.of(
        "R__create_topic.sql@1", "1928751b22bcc9475130e2756c3525b98b4925aeb978cc9508c801888cb6391a",
        "R__notify_limit.sql@1", "226a96d4b8897dd34af1e89b36758b957e8196d00e50d50c87711678fea5eeb1",
        "R__notify_ready.sql@1", "5c0746501fb02bf45e5dcffe6a496a41dca777abb1a9363aebec0981f7d9d8d5",
        "R__notify_topic.sql@1", "b5e2e0b151cac20418b6f4b30e34fb369a765f64fe2407edff3710b5592ec1a4",
        "R__task_view.sql@1", "5b1ceb6291c454ce53142a7cfc1d1d3940bb98bf2d87e463429537ea239109ae"
    );

    @Test
    void every_repeatable_revision_is_pinned() {
        for (SchemaScript script : TaskSchema.scripts()) {
            if (!script.repeatable()) {
                continue;
            }
            String key = script.name() + "@" + script.revision();
            assertThat(script.revision()).as(script.name() + " carries a '-- revision: <n>' header of 1 or more").isPositive();
            assertThat(REVISIONS)
                .as(key + " (checksum " + script.checksum() + "): every change to a repeatable file must raise the revision in its header, "
                    + "and each new revision's checksum goes into SchemaFreezeTest.REVISIONS")
                .containsEntry(key, script.checksum());
            REVISIONS.keySet().stream().filter(each -> each.startsWith(script.name() + "@"))
                .forEach(each -> assertThat(Integer.parseInt(each.substring(each.indexOf('@') + 1)))
                    .as(each + " is pinned, so the shipped file can't go back to it or below").isLessThanOrEqualTo(script.revision()));
        }
    }

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
