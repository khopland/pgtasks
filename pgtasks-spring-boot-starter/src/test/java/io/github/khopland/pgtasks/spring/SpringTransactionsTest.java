package io.github.khopland.pgtasks.spring;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.spi.AppTransaction;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Review 2026-11-24: each after-commit action was a synchronization of its own, and Spring stops
 * at the first that throws, so on the in-memory store one failed push dropped the later ones.
 */
class SpringTransactionsTest {

    private final SpringTransactions transactions = new SpringTransactions(null);
    private final TransactionTemplate template = new TransactionTemplate(new TaskAutoConfigurationTest.NoResources());

    @Test
    void every_action_runs_after_the_commit_even_when_one_fails() {
        List<String> ran = new CopyOnWriteArrayList<>();

        assertThatThrownBy(() -> template.executeWithoutResult(status -> {
            AppTransaction transaction = transactions.current().orElseThrow();
            transaction.afterCommit(() -> ran.add("first"));
            transaction.afterCommit(() -> {
                throw new IllegalStateException("second failed");
            });
            transaction.afterCommit(() -> ran.add("third"));
            transaction.afterCommit(() -> {
                throw new IllegalArgumentException("fourth failed");
            });
        }))
            .hasMessage("second failed")
            .satisfies(failure -> assertThat(failure.getSuppressed()).singleElement().extracting(Throwable::getMessage).isEqualTo("fourth failed"));

        assertThat(ran).containsExactly("first", "third");
    }

    @Test
    void an_inner_transaction_of_its_own_runs_its_actions_at_its_own_commit() {
        List<String> ran = new CopyOnWriteArrayList<>();
        TransactionTemplate inner = new TransactionTemplate(template.getTransactionManager());
        inner.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        template.executeWithoutResult(status -> {
            transactions.current().orElseThrow().afterCommit(() -> ran.add("outer"));
            inner.executeWithoutResult(innerStatus -> transactions.current().orElseThrow().afterCommit(() -> ran.add("inner")));
            assertThat(ran).containsExactly("inner");
        });

        assertThat(ran).containsExactly("inner", "outer");
    }

    @Test
    void a_rolled_back_transaction_runs_none() {
        List<String> ran = new CopyOnWriteArrayList<>();
        template.executeWithoutResult(status -> {
            transactions.current().orElseThrow().afterCommit(() -> ran.add("never"));
            status.setRollbackOnly();
        });
        assertThat(ran).isEmpty();
    }
}
