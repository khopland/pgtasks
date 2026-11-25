package io.github.khopland.pgtasks;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Review 2026-11-25: identifiers, references and dedup keys had no length limit, and Postgres
 * can't index one much over 2.7 KB. A longer one failed the push, and staged by a handler, the
 * completion of its whole batch: every task in it was retried and, in the end, dead-lettered.
 */
class KeysTest {

    @Test
    void a_key_too_long_to_index_is_refused_where_it_is_made() {
        String tooLong = "k".repeat(Creation.MAX_KEY_BYTES + 1);
        assertThatThrownBy(() -> Creation.of(tooLong, "x"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("identifier is 2049 bytes");
        assertThatThrownBy(() -> Creation.of("a", "x").withReference(tooLong)).hasMessageContaining("reference is 2049 bytes");
        assertThatThrownBy(() -> Creation.of("a", "x").withDedupKey(tooLong)).hasMessageContaining("dedupKey is 2049 bytes");
    }

    @Test
    void the_limit_counts_bytes_of_utf8_not_characters() {
        assertThat(Creation.of("k".repeat(Creation.MAX_KEY_BYTES), "x").identifier()).hasSize(Creation.MAX_KEY_BYTES);
        String twoBytes = "æ".repeat(Creation.MAX_KEY_BYTES / 2);
        assertThat(Creation.of(twoBytes, "x").identifier()).isEqualTo(twoBytes);
        assertThatThrownBy(() -> Creation.of(twoBytes + "æ", "x")).hasMessageContaining("2050 bytes");
        String fourBytes = "😀".repeat(Creation.MAX_KEY_BYTES / 4);
        assertThat(Creation.of("a", "x").withReference(fourBytes).reference()).isEqualTo(fourBytes);
        assertThatThrownBy(() -> Creation.of("a", "x").withReference(fourBytes + "😀")).hasMessageContaining("2052 bytes");
    }
}
