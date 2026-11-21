package io.github.khopland.pgtasks.test.tck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.admin.LimitOverride;
import io.github.khopland.pgtasks.spi.TaskStore;
import org.junit.jupiter.api.Test;

import org.jspecify.annotations.Nullable;

/** TCK: the strict rate limit, a token bucket shared by every node, and operators' overrides of it. */
public abstract class AdmissionTck extends StoreTck {

    private final TaskStore.Admission limit = new TaskStore.Admission("downstream", 10, 3);
    private int topic, node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
        fixture.inTransaction(transaction -> {
            fixture.store().ensureAdmission(transaction, limit.name(), limit.burst());
            return null;
        });
        return fixture;
    }

    private TaskStore.Claim claim(StoreFixture fixture, int node, int size) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().claim(transaction, new TaskStore.ClaimRequest(topic, node, Ordering.FIFO, size, limit)));
    }

    @Test
    void takes_permits_before_polling_and_waits_when_none_are_left() throws SQLException {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));

        TaskStore.Claim first = claim(fixture, node, 100);
        TaskStore.Claim second = claim(fixture, node, 100);

        assertThat(first.tasks()).as("the burst").hasSize(3);
        assertThat(second.tasks()).isEmpty();
        assertThat(second.admissionWait()).isPositive().isLessThanOrEqualTo(Duration.ofMillis(110));
        assertThat(fixture.counts(topic).active()).isEqualTo(3);
    }

    @Test
    void empty_polls_are_not_charged() throws SQLException {
        StoreFixture fixture = setUp();
        assertThat(claim(fixture, node, 100).tasks()).isEmpty();
        assertThat(claim(fixture, node, 100).tasks()).isEmpty();
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 5));

        assertThat(claim(fixture, node, 100).tasks()).hasSize(3);
    }

    @Test
    void refills_at_the_configured_rate() throws Exception {
        StoreFixture fixture = setUp();
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));
        assertThat(claim(fixture, node, 100).tasks()).hasSize(3);

        fixture.advance(Duration.ofMillis(250));

        assertThat(claim(fixture, node, 100).tasks()).hasSizeBetween(2, 3);
    }

    @Test
    void never_holds_more_than_the_burst() throws Exception {
        StoreFixture fixture = setUp();
        fixture.advance(Duration.ofHours(1));
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));

        assertThat(claim(fixture, node, 100).tasks()).hasSize(3);
    }

    @Test
    void the_bucket_is_shared_by_every_node() throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.node("node-b");
        fixture.push(topic, Insertion.APPEND, StoreFixture.many("t", 20));

        List<TaskStore.ClaimedTask> mine = claim(fixture, node, 2).tasks();
        List<TaskStore.ClaimedTask> theirs = claim(fixture, other, 100).tasks();

        assertThat(mine.size() + theirs.size()).isEqualTo(3);
    }

    private static List<LimitOverride> overrides(StoreFixture fixture) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().limitOverrides(transaction));
    }

    private Optional<LimitOverride> override(StoreFixture fixture, @Nullable Double rate, @Nullable Integer burst, @Nullable Long expected) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().overrideLimit(transaction, limit.name(), rate, burst, null, "ops", expected));
    }

    @Test
    void overrides_are_versioned_so_two_operators_cannot_overwrite_each_other() throws SQLException {
        StoreFixture fixture = setUp();
        assertThat(overrides(fixture)).isEmpty();

        LimitOverride first = override(fixture, 20.0, null, 0L).orElseThrow();
        Optional<LimitOverride> stale = override(fixture, 5.0, null, 0L);
        LimitOverride second = override(fixture, null, 8, first.version()).orElseThrow();
        LimitOverride forced = override(fixture, 30.0, 9, null).orElseThrow();

        assertThat(first.version()).isEqualTo(1);
        assertThat(first.perSecond()).isEqualTo(20.0);
        assertThat(first.burst()).as("keeps the configured burst").isNull();
        assertThat(first.updatedBy()).isEqualTo("ops");
        assertThat(stale).as("based on a version that is gone").isEmpty();
        assertThat(second.version()).isEqualTo(2);
        assertThat(second.perSecond()).as("a change replaces the whole override").isNull();
        assertThat(forced.version()).as("no version given: changes it regardless").isEqualTo(3);
        assertThat(override(fixture, 1.0, null, 7L)).isEmpty();
        assertThat(overrides(fixture)).containsExactly(forced);
    }

    @Test
    void a_first_override_must_be_based_on_version_zero() throws SQLException {
        StoreFixture fixture = setUp();
        assertThat(override(fixture, 20.0, null, 3L)).isEmpty();
        assertThat(override(fixture, 20.0, null, 0L)).isPresent();
    }

    @Test
    void a_reset_keeps_the_row_so_the_version_only_grows() throws SQLException {
        StoreFixture fixture = setUp();
        LimitOverride set = override(fixture, 20.0, 5, 0L).orElseThrow();
        LimitOverride reset = override(fixture, null, null, set.version()).orElseThrow();

        assertThat(reset.version()).isEqualTo(2);
        assertThat(reset.activeAt(fixture.store().clock().instant())).isFalse();
        assertThat(override(fixture, 20.0, null, 0L)).as("version 0 is gone for good").isEmpty();
    }

    @Test
    void an_override_lapses_at_its_until() throws Exception {
        StoreFixture fixture = setUp();
        Instant until = fixture.store().clock().instant().plusSeconds(60);
        fixture.inTransaction(transaction -> fixture.store().overrideLimit(transaction, limit.name(), 20.0, null, until, "ops", null));

        fixture.advance(Duration.ofSeconds(61));

        LimitOverride override = overrides(fixture).getFirst();
        assertThat(override.activeAt(fixture.store().clock().instant())).isFalse();
    }

    @Test
    void only_a_configured_limit_can_be_overridden() throws SQLException {
        StoreFixture fixture = setUp();
        assertThatThrownBy(() -> fixture.inTransaction(transaction -> fixture.store().overrideLimit(transaction, "unknown", 1.0, null, null, "ops", null)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unknown");
    }

    @Test
    void an_override_in_a_rolled_back_transaction_is_not_kept() throws SQLException {
        StoreFixture fixture = setUp();
        try (var transaction = fixture.store().begin()) {
            fixture.store().overrideLimit(transaction, limit.name(), 20.0, null, null, "ops", null);
            transaction.rollback();
        }
        assertThat(overrides(fixture)).isEmpty();
    }
}
