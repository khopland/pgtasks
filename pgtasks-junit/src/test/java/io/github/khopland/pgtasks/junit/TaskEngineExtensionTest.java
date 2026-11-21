package io.github.khopland.pgtasks.junit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.LocalTransactions;
import io.github.khopland.pgtasks.engine.TaskConfigurer;
import io.github.khopland.pgtasks.engine.TaskTester;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TaskEngineExtensionTest {

    static final TopicRef<String> NOTIFY = TopicRef.of("notify", String.class);
    static final TopicRef<String> ARCHIVE = TopicRef.of("archive", String.class);

    /** Fails each task twice before it goes through, as a flaky downstream system would. */
    static final Map<String, AtomicInteger> tries = new ConcurrentHashMap<>();

    static final TaskConfigurer NOTIFY_TASKS = tasks -> tasks
        .eachTask(NOTIFY, task -> {
            if (tries.computeIfAbsent(task.identifier(), ignored -> new AtomicInteger()).incrementAndGet() < 3) {
                throw new IllegalStateException("mail server busy");
            }
            task.push(ARCHIVE, task.identifier(), "sent " + task.payload());
        })
        .pushesTo(ARCHIVE);

    @RegisterExtension
    static TaskEngineExtension engine = TaskEngineExtension.of(NOTIFY_TASKS);

    @Test
    void a_push_in_a_transaction_runs_and_its_retries_are_jumped_through(Tasks tasks, TaskTester tester, LocalTransactions transactions)
        throws Exception {
        transactions.inTransaction(() -> tasks.push(NOTIFY, "anna", "hello"));

        tester.drain(NOTIFY);

        assertThat(tries.get("anna")).hasValue(3);
        tester.assertPushed(ARCHIVE, 1);
        assertThat(tester.pushedPayloads(ARCHIVE)).containsExactly("sent hello");
        assertThatThrownBy(() -> tester.assertPushed(ARCHIVE, 2)).hasMessage("Expected 2 push(es) to archive, but 1 committed, for identifiers [anna]");
    }

    @Test
    void every_test_gets_a_fresh_engine(TaskTester tester) {
        tester.assertNothingPushed(ARCHIVE);
        assertThat(engine.admin().topics()).extracting(status -> status.counts() == null ? 0 : status.counts().total()).containsOnly(0L);
    }
}
