package io.github.khopland.pgtasks.test.tck;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Junction;
import io.github.khopland.pgtasks.Pushed;

import org.junit.jupiter.api.Test;

import static io.github.khopland.pgtasks.test.tck.StoreFixture.suspended;
import static io.github.khopland.pgtasks.test.tck.StoreFixture.task;
import static org.assertj.core.api.Assertions.assertThat;

/** TCK: fan-in. A complete group releases its suspended tasks; nothing else changes. */
public abstract class JunctionTck extends StoreTck {

    private int topic, other, node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("merge");
        other = fixture.topic("other");
        node = fixture.node("node-a");
        return fixture;
    }

    private long parked(StoreFixture fixture, int topic, String identifier) throws SQLException {
        return StoreFixture.id(fixture.push(topic, Insertion.APPEND, suspended(identifier)).getFirst());
    }

    private Set<String> junction(StoreFixture fixture, Junction.Counts counts, Collection<Set<String>> groups) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().junction(transaction, topic, counts, groups));
    }

    @Test
    void singular_releases_a_group_once_every_member_is_suspended() throws SQLException {
        StoreFixture fixture = setUp();
        long a = parked(fixture, topic, "a");
        long b = parked(fixture, topic, "b");

        assertThat(junction(fixture, Junction.Counts.SUSPENDED, List.of(Set.of("a", "b", "c")))).as("c is missing").isEmpty();
        assertThat(List.of(a, b)).extracting(fixture::state).containsOnly("suspended");

        assertThat(junction(fixture, Junction.Counts.SUSPENDED, List.of(Set.of("a", "b")))).containsExactlyInAnyOrder("a", "b");
        assertThat(List.of(a, b)).extracting(fixture::state).containsOnly("ready");
    }

    @Test
    void singular_only_counts_suspended_tasks() throws SQLException {
        StoreFixture fixture = setUp();
        long a = parked(fixture, topic, "a");
        fixture.push(topic, "b");

        assertThat(junction(fixture, Junction.Counts.SUSPENDED, List.of(Set.of("a", "b")))).isEmpty();
        assertThat(fixture.state(a)).isEqualTo("suspended");
    }

    @Test
    void eternal_counts_any_task_finished_or_not() throws SQLException {
        StoreFixture fixture = setUp();
        long a = parked(fixture, topic, "a");
        fixture.push(topic, "b");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.succeeded());
        fixture.push(topic, "c");

        assertThat(junction(fixture, Junction.Counts.ANY, List.of(Set.of("a", "b", "c")))).containsExactly("a");
        assertThat(fixture.state(a)).isEqualTo("ready");
    }

    @Test
    void eternal_does_not_count_a_redundant_task() throws SQLException {
        StoreFixture fixture = setUp();
        long a = parked(fixture, topic, "a");
        fixture.push(topic, Insertion.APPEND, StoreFixture.deduplicated("holder", "key"));
        // The first r is stored as redundant; the second is a duplicate and not stored at all.
        assertThat(fixture.push(topic, Insertion.SUPERSEDE, task("r"), StoreFixture.deduplicated("r", "key")))
            .extracting(Pushed::status)
            .containsExactly(Pushed.Status.REDUNDANT, Pushed.Status.DUPLICATE);

        assertThat(junction(fixture, Junction.Counts.ANY, List.of(Set.of("a", "r")))).isEmpty();
        assertThat(fixture.state(a)).isEqualTo("suspended");
    }

    @Test
    void groups_are_checked_one_by_one() throws SQLException {
        StoreFixture fixture = setUp();
        long a = parked(fixture, topic, "a");
        long b = parked(fixture, topic, "b");
        long c = parked(fixture, topic, "c");

        assertThat(junction(fixture, Junction.Counts.SUSPENDED, List.of(Set.of("a", "b"), Set.of("c", "missing")))).containsExactlyInAnyOrder("a", "b");
        assertThat(List.of(a, b, c)).extracting(fixture::state).containsExactly("ready", "ready", "suspended");
    }

    @Test
    void only_suspended_tasks_are_released() throws SQLException {
        StoreFixture fixture = setUp();
        long parkedA = parked(fixture, topic, "a");
        fixture.push(topic, Insertion.APPEND, StoreFixture.delayed("a", Duration.ofHours(1)));
        long dead = fixture.push(topic, "dead");
        fixture.complete(topic, node, fixture.claim(topic, node).getFirst(), StoreFixture.deadLetter());
        long parkedDead = parked(fixture, topic, "dead");

        assertThat(junction(fixture, Junction.Counts.ANY, List.of(Set.of("a", "dead")))).containsExactlyInAnyOrder("a", "dead");
        assertThat(fixture.state(parkedA)).isEqualTo("ready");
        assertThat(fixture.state(parkedDead)).isEqualTo("ready");
        assertThat(fixture.state(dead)).isEqualTo("failed");
        assertThat(fixture.counts(topic).scheduled()).isEqualTo(1);
    }

    @Test
    void only_tasks_in_the_target_topic_count() throws SQLException {
        StoreFixture fixture = setUp();
        long a = parked(fixture, topic, "a");
        parked(fixture, other, "b");

        assertThat(junction(fixture, Junction.Counts.SUSPENDED, List.of(Set.of("a", "b")))).isEmpty();
        assertThat(fixture.state(a)).isEqualTo("suspended");
    }
}
