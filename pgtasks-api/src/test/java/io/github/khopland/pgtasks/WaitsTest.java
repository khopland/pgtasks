package io.github.khopland.pgtasks;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Review 2026-11-24: delays and run times had no upper bound. One the store couldn't add to its
 * clock, such as {@code Instant.MAX} meant as "never", failed the write: for a decision or
 * downstream work, the completion of the whole batch, on every retry.
 */
class WaitsTest {

    @Test
    void a_wait_the_store_cant_hold_is_refused_where_it_is_made() {
        assertThatThrownBy(() -> Decision.later(Duration.ofSeconds(Long.MAX_VALUE)))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at most a thousand years");
        assertThatThrownBy(() -> Creation.of("a", "x").withDelay(Duration.ofDays(400_000)))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at most a thousand years");
        assertThatThrownBy(() -> Creation.of("a", "x").withRunAt(Instant.MAX))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("years 1 to 9999");
        assertThatThrownBy(() -> Creation.of("a", "x").withRunAt(Instant.MIN))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("years 1 to 9999");
        assertThatThrownBy(() -> Decision.later(Duration.ofSeconds(-1))).hasMessageContaining("must not be negative");
    }

    @Test
    void long_waits_within_the_bounds_are_fine() {
        assertThat(Decision.later(Waits.MAX_DELAY)).isEqualTo(new Decision.Later(Waits.MAX_DELAY));
        assertThat(Creation.of("a", "x").withDelay(Duration.ofDays(36_500)).delay()).isEqualTo(Duration.ofDays(36_500));
        assertThat(Creation.of("a", "x").withRunAt(Waits.LATEST).runAt()).isEqualTo(Waits.LATEST);
        assertThat(Creation.of("a", "x").withRunAt(Instant.EPOCH).runAt()).isEqualTo(Instant.EPOCH);
    }
}
