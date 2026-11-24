package io.github.khopland.pgtasks.engine;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Review 2026-11-24: the first after-commit action that failed skipped the rest. On a store that
 * doesn't join the application's transactions those actions store the pushes, so one failed push
 * silently dropped every later push of the transaction.
 */
class AfterCommitTest {

    @Test
    void every_action_runs_after_the_commit_even_when_one_fails() {
        List<String> ran = new CopyOnWriteArrayList<>();
        LocalTransactions transactions = new LocalTransactions();

        assertThatThrownBy(() -> transactions.inTransaction(() -> {
            transactions.current().orElseThrow().afterCommit(() -> ran.add("first"));
            transactions.current().orElseThrow().afterCommit(() -> {
                throw new IllegalStateException("second failed");
            });
            transactions.current().orElseThrow().afterCommit(() -> ran.add("third"));
            transactions.current().orElseThrow().afterCommit(() -> {
                throw new IllegalArgumentException("fourth failed");
            });
            return null;
        }))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("committed")
            .cause().hasMessage("second failed")
            .satisfies(cause -> assertThat(cause.getSuppressed()).singleElement().extracting(Throwable::getMessage).isEqualTo("fourth failed"));

        assertThat(ran).containsExactly("first", "third");
    }
}
