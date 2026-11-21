package io.github.khopland.pgtasks.test.tck;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.spi.TaskStore.ClaimedTask;
import org.junit.jupiter.api.Test;

import org.jspecify.annotations.Nullable;

/** TCK: listing, reviving and discarding dead letters. */
public abstract class DeadLetterTck extends StoreTck {

    private int topic;
    private int node;

    private StoreFixture setUp() throws SQLException {
        StoreFixture fixture = fixture();
        topic = fixture.topic("topic");
        node = fixture.node("node-a");
        return fixture;
    }

    /** Pushes a task and runs it into the dead letters. */
    private long deadLetter(StoreFixture fixture, int topic, String identifier) throws SQLException {
        long id = fixture.push(topic, identifier);
        ClaimedTask claimed = fixture.claim(topic, node).stream().filter(task -> task.id() == id).findFirst().orElseThrow();
        fixture.complete(topic, node, claimed, StoreFixture.deadLetter());
        return id;
    }

    private List<TaskView> deadLetters(StoreFixture fixture, int topic, long after, int limit) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().deadLetters(transaction, topic, after, limit));
    }

    private List<Long> revive(StoreFixture fixture, @Nullable Collection<Long> ids, int limit) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().revive(transaction, topic, ids, limit));
    }

    private List<Long> discard(StoreFixture fixture, @Nullable Collection<Long> ids, int limit) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().discard(transaction, topic, ids, limit));
    }

    @Test
    void dead_letters_are_listed_lowest_id_first_with_their_error_one_page_at_a_time() throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.topic("other");
        long first = deadLetter(fixture, topic, "a");
        fixture.push(topic, "still-queued");
        long second = deadLetter(fixture, topic, "b");
        long third = deadLetter(fixture, topic, "c");
        deadLetter(fixture, other, "elsewhere");

        List<TaskView> page = deadLetters(fixture, topic, 0, 2);
        assertThat(page).extracting(TaskView::id).containsExactly(first, second);
        assertThat(page.getFirst().state()).isEqualTo("failed");
        assertThat(page.getFirst().finished()).isFalse();
        assertThat(page.getFirst().error()).contains("gave up");
        assertThat(page.getFirst().topic()).isEqualTo("topic");
        assertThat(deadLetters(fixture, topic, second, 2)).extracting(TaskView::id).containsExactly(third);
    }

    @Test
    void a_revived_dead_letter_is_ready_and_starts_its_attempts_again() throws SQLException {
        StoreFixture fixture = setUp();
        long id = deadLetter(fixture, topic, "a");

        assertThat(revive(fixture, null, 100)).containsExactly(id);

        assertThat(fixture.state(id)).isEqualTo("ready");
        ClaimedTask again = fixture.claim(topic, node).getFirst();
        assertThat(again.id()).isEqualTo(id);
        assertThat(again.attempt()).isEqualTo(1);
        assertThat(fixture.attempts(id)).as("the earlier failure stays in the log").hasSize(1);
        assertThat(fixture.counts(topic).failed()).isZero();
        assertThat(fixture.view(id).orElseThrow().failures()).as("a revived task starts afresh").isZero();
        assertThat(fixture.view(id).orElseThrow().firstFailedAt()).isNull();
    }

    @Test
    void reviving_by_id_skips_what_is_not_a_dead_letter_of_the_topic() throws SQLException {
        StoreFixture fixture = setUp();
        int other = fixture.topic("other");
        long dead = deadLetter(fixture, topic, "a");
        long queued = fixture.push(topic, "b");
        long elsewhere = deadLetter(fixture, other, "c");

        assertThat(revive(fixture, List.of(dead, queued, elsewhere, 999_999L), 100)).containsExactly(dead);

        assertThat(fixture.state(elsewhere)).isEqualTo("failed");
    }

    @Test
    void dead_letters_are_found_by_error_by_when_they_first_failed_and_after_an_id() throws SQLException {
        StoreFixture fixture = setUp();
        long gaveUp = deadLetter(fixture, topic, "a");
        long refusedId = fixture.push(topic, "b");
        ClaimedTask claimed = fixture.claim(topic, node).stream().filter(task -> task.id() == refusedId).findFirst().orElseThrow();
        fixture.complete(topic, node, claimed, new io.github.khopland.pgtasks.spi.TaskStore.Resolution.Waiting(
            io.github.khopland.pgtasks.spi.TaskStore.WaitState.DEAD_LETTER, null, true, "{\"message\":\"refused\",\"type\":\"java.net.ConnectException\"}"));
        long later = deadLetter(fixture, topic, "c");
        DeadLetterQuery all = DeadLetterQuery.ALL;
        java.time.Instant now = fixture.store().clock().instant();

        assertThat(find(fixture, all.withError("java.net.ConnectException"))).containsExactly(refusedId);
        assertThat(find(fixture, all.withError("gave up"))).containsExactly(gaveUp, later);
        assertThat(find(fixture, all.failedBetween(now.plusSeconds(3600), null))).isEmpty();
        assertThat(find(fixture, all.failedBetween(now.minusSeconds(3600), now.plusSeconds(3600)))).hasSize(3);

        List<Long> revived = fixture.inTransaction(transaction -> fixture.store().revive(transaction, topic, all.withError("gave up").after(gaveUp), 10));
        assertThat(revived).containsExactly(later);
        assertThat(fixture.state(gaveUp)).isEqualTo("failed");
        List<Long> discarded = fixture.inTransaction(transaction -> fixture.store().discard(transaction, topic, all.withError("java.net.ConnectException"), 10));
        assertThat(discarded).containsExactly(refusedId);
    }

    private List<Long> find(StoreFixture fixture, DeadLetterQuery query) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().deadLetters(transaction, topic, query, 10)).stream().map(TaskView::id).toList();
    }

    @Test
    void revival_is_claimed_once_per_version_and_a_sweep_once_per_interval() throws Exception {
        StoreFixture fixture = setUp();
        assertThat(claimRevival(fixture, "1.0")).as("the first node of 1.0").isTrue();
        assertThat(claimRevival(fixture, "1.0")).as("every other node of 1.0").isFalse();
        assertThat(claimRevival(fixture, "1.1")).isTrue();
        assertThat(claimRevival(fixture, "1.0")).as("going back is a new version too").isTrue();

        assertThat(claimSweep(fixture)).isTrue();
        assertThat(claimSweep(fixture)).isFalse();
        fixture.advance(java.time.Duration.ofHours(2));
        assertThat(claimSweep(fixture)).isTrue();
    }

    private boolean claimRevival(StoreFixture fixture, String version) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().claimRevival(transaction, topic, version));
    }

    private boolean claimSweep(StoreFixture fixture) throws SQLException {
        return fixture.inTransaction(transaction -> fixture.store().claimReviveSweep(transaction, topic, java.time.Duration.ofHours(1)));
    }

    @Test
    void reviving_all_takes_the_oldest_up_to_the_limit() throws SQLException {
        StoreFixture fixture = setUp();
        long first = deadLetter(fixture, topic, "a");
        long second = deadLetter(fixture, topic, "b");
        long third = deadLetter(fixture, topic, "c");

        assertThat(revive(fixture, null, 2)).containsExactly(first, second);
        assertThat(fixture.state(third)).isEqualTo("failed");
    }

    @Test
    void a_discarded_dead_letter_finishes_as_failed_and_keeps_its_error() throws SQLException {
        StoreFixture fixture = setUp();
        long id = deadLetter(fixture, topic, "a");

        assertThat(discard(fixture, List.of(id), 100)).containsExactly(id);

        TaskView view = fixture.view(id).orElseThrow();
        assertThat(view.finished()).isTrue();
        assertThat(view.state()).isEqualTo("failed");
        assertThat(view.completedAt()).isNotNull();
        assertThat(view.error()).contains("gave up");
        assertThat(view.attempt()).isEqualTo(1);
        assertThat(fixture.counts(topic).total()).isZero();
        assertThat(discard(fixture, List.of(id), 100)).as("discarding twice does nothing").isEmpty();
    }

    @Test
    void discarding_a_dead_letter_lets_the_tasks_it_held_back_run() throws SQLException {
        StoreFixture fixture = setUp();
        long dead = deadLetter(fixture, topic, "a");
        long next = StoreFixture.id(fixture.push(topic, Insertion.APPEND, StoreFixture.task("a")).getFirst());
        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER_ON_FAILURE, 10))
            .as("held back by the dead letter").isEmpty();

        discard(fixture, List.of(dead), 100);

        assertThat(fixture.claim(topic, node, Ordering.SINGULAR_BY_IDENTIFIER_ON_FAILURE, 10)).extracting(ClaimedTask::id).containsExactly(next);
    }
}
