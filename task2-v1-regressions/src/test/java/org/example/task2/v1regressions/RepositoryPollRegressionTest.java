package org.example.task2.v1regressions;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.util.Map;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSink;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.PostgresTaskRepository;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Findings #6 and #7 (plan section 02), against the 1.x Postgres repository. The expected
 * behaviour is the per-identifier ordering contract in plan section 10: only earlier tasks in
 * the same topic can hold a task back.
 */
class RepositoryPollRegressionTest {

    @RegisterExtension
    static PostgresExtension postgres = LegacySchema.extension();

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = TaskSource.Condition.class, names = "SINGULAR_BY_IDENTIFIER.*", mode = EnumSource.Mode.MATCH_ALL)
    @DisplayName("#6 a ready task in another topic does not block the same identifier")
    void finding_6_identifier_condition_is_scoped_to_topic(TaskSource.Condition condition, PostgresDatabase database) throws Exception {
        PostgresTaskRepository repository = new PostgresTaskRepository(true, "node-a");
        try (Connection connection = database.connect()) {
            repository.register(connection);
            repository.initialize(connection, "first");
            repository.initialize(connection, "second");
            repository.push(connection, "first", TaskSink.Insertion.APPEND, "person-1");
            Task second = repository.push(connection, "second", TaskSink.Insertion.APPEND, "person-1");

            assertThat(repository.poll(connection, "second", TaskSource.Order.FIRST_IN_FIRST_OUT, condition, 10))
                .extracting(Task::getSequence)
                .containsExactly(second.getSequence());
        }
    }

    static Stream<Arguments> laterBlockers() {
        return Stream.of(
            Arguments.of(TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_ON_FAILURE, TaskDecision.FAILURE),
            Arguments.of(TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_UNTIL_SUCCESS, TaskDecision.FAILURE),
            Arguments.of(TaskSource.Condition.SINGULAR_BY_IDENTIFIER_SUSPEND_UNTIL_SUCCESS, TaskDecision.SUSPENSION)
        );
    }

    @ParameterizedTest(name = "{0} with a later {1}")
    @MethodSource("laterBlockers")
    @DisplayName("#7 a failed or suspended task only blocks the tasks after it")
    void finding_7_failure_checks_respect_task_order(TaskSource.Condition condition, TaskDecision later, PostgresDatabase database) throws Exception {
        PostgresTaskRepository repository = new PostgresTaskRepository(true, "node-a");
        try (Connection connection = database.connect()) {
            repository.register(connection);
            repository.initialize(connection, "topic");
            Task first = repository.push(connection, "topic", TaskSink.Insertion.APPEND, "person-1");
            Task second = repository.push(connection, "topic", TaskSink.Insertion.APPEND, "person-1");
            assertThat(repository.poll(connection, "topic", TaskSource.Order.LAST_IN_FIRST_OUT, TaskSource.Condition.NONE, 1))
                .containsExactly(second);
            repository.complete(connection, "topic", Map.of(second, later));

            assertThat(repository.poll(connection, "topic", TaskSource.Order.FIRST_IN_FIRST_OUT, condition, 10))
                .as("the earlier task must not wait for a later %s task", later.getResult())
                .extracting(Task::getSequence)
                .containsExactly(first.getSequence());
        }
    }
}
