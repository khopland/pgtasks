package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.github.khopland.pgtasks.spi.ExtensionEntry;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** TCK: the entries extensions keep in the store, by extension and key. */
public abstract class ExtensionDataTck extends StoreTck {

    private TaskStore store() {
        return fixture().store();
    }

    private Optional<ExtensionEntry> put(String extension, String key, String value, Long version) throws SQLException {
        return fixture().inTransaction(transaction -> store().putExtensionEntry(transaction, extension, key, value, version));
    }

    private ExtensionEntry put(String extension, String key, String value) throws SQLException {
        return fixture().inTransaction(transaction -> store().putExtensionEntry(transaction, extension, key, value, null)).orElseThrow();
    }

    private Optional<ExtensionEntry> get(String extension, String key) throws SQLException {
        return fixture().inTransaction(transaction -> store().extensionEntry(transaction, extension, key));
    }

    private boolean delete(String extension, String key, @Nullable Long version) throws SQLException {
        Boolean deleted = fixture().inTransaction(transaction -> store().deleteExtensionEntry(transaction, extension, key, version));
        return deleted;
    }

    private List<String> keys(String extension, String prefix, @Nullable String after, int limit) throws SQLException {
        return fixture().inTransaction(transaction -> store().extensionEntries(transaction, extension, prefix, after, limit))
            .stream().map(ExtensionEntry::key).toList();
    }

    @Test
    void an_entry_is_written_read_and_versioned() throws Exception {
        ExtensionEntry first = put("pause", "orders", "paused");
        assertThat(first.key()).isEqualTo("orders");
        assertThat(first.value()).isEqualTo("paused");
        assertThat(first.version()).isEqualTo(1);
        assertThat(first.updatedAt()).isNotNull();
        assertThat(get("pause", "orders")).contains(first);

        fixture().advance(Duration.ofMinutes(1));
        ExtensionEntry second = put("pause", "orders", "paused again");
        assertThat(second.version()).isEqualTo(2);
        assertThat(second.updatedAt()).isAfter(first.updatedAt());
        assertThat(get("pause", "orders").map(ExtensionEntry::value)).contains("paused again");

        assertThat(get("pause", "payments")).isEmpty();
        assertThat(get("other", "orders")).as("each extension has keys of its own").isEmpty();
    }

    @Test
    void a_write_at_a_version_succeeds_only_at_that_version() throws SQLException {
        assertThat(put("quota", "north", "1", 0L)).as("absent, so version 0 matches").map(ExtensionEntry::version).contains(1L);
        assertThat(put("quota", "north", "1", 0L)).as("present now").isEmpty();
        assertThat(put("quota", "north", "2", 1L)).map(ExtensionEntry::value).contains("2");
        assertThat(put("quota", "north", "3", 1L)).as("someone wrote in between").isEmpty();
        assertThat(put("quota", "south", "1", 4L)).as("absent, not at version 4").isEmpty();
        assertThat(get("quota", "north").map(ExtensionEntry::value)).contains("2");
    }

    @Test
    void an_entry_is_deleted_at_any_version_or_only_at_one() throws SQLException {
        put("quota", "north", "1");
        put("quota", "north", "2");
        assertThat(delete("quota", "north", 1L)).isFalse();
        assertThat(delete("quota", "north", 2L)).isTrue();
        assertThat(get("quota", "north")).isEmpty();

        put("quota", "north", "again");
        assertThat(get("quota", "north").map(ExtensionEntry::version)).as("a new entry starts again at 1").contains(1L);
        assertThat(delete("quota", "north", null)).isTrue();
        assertThat(delete("quota", "north", null)).isFalse();
    }

    @Test
    void entries_are_listed_by_prefix_in_key_order_a_page_at_a_time() throws SQLException {
        for (String key : List.of("schedule/b", "schedule/a", "schedule/c", "schedules", "other/a", "schedule_x", "schedule/😀", "schedule/�")) {
            put("recurring", key, key);
        }
        put("elsewhere", "schedule/a", "not this extension's");

        assertThat(keys("recurring", "schedule/", null, 10)).as("by code point: U+FFFD before U+1F600")
            .containsExactly("schedule/a", "schedule/b", "schedule/c", "schedule/�", "schedule/😀");
        assertThat(keys("recurring", "schedule/", null, 2)).containsExactly("schedule/a", "schedule/b");
        assertThat(keys("recurring", "schedule/", "schedule/b", 2)).containsExactly("schedule/c", "schedule/�");
        assertThat(keys("recurring", "schedule_", null, 10)).as("an underscore in the prefix is just an underscore").containsExactly("schedule_x");
        assertThat(keys("recurring", "", null, 100)).hasSize(8);
        assertThat(keys("recurring", "schedule/", "other", 10)).as("after a key before the prefix").hasSize(5);
        assertThat(keys("recurring", "nothing/", null, 10)).isEmpty();
    }

    @Test
    void writes_roll_back_with_their_transaction_and_savepoint() throws SQLException {
        put("pause", "kept", "before");
        try (StoreTransaction transaction = store().begin()) {
            store().putExtensionEntry(transaction, "pause", "kept", "changed", null);
            store().putExtensionEntry(transaction, "pause", "added", "new", null);
            store().deleteExtensionEntry(transaction, "pause", "kept", null);
            transaction.rollback();
        }
        assertThat(get("pause", "kept").map(ExtensionEntry::value)).contains("before");
        assertThat(get("pause", "added")).isEmpty();

        fixture().inTransaction(transaction -> {
            store().putExtensionEntry(transaction, "pause", "first", "kept", null);
            StoreTransaction.Savepoint savepoint = transaction.savepoint();
            store().putExtensionEntry(transaction, "pause", "second", "undone", null);
            savepoint.rollback();
            return null;
        });
        assertThat(get("pause", "first")).isPresent();
        assertThat(get("pause", "second")).isEmpty();
    }
}
