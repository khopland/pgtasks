package org.example.task2.v1regressions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskJuncture;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResult;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSink;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.CompositeTaskContext;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskCompletion;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskConsumerContext;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Findings #8 and #9 (plan section 02), against the 1.x task contexts. */
class TaskContextRegressionTest {

    private final RecordingSink sink = new RecordingSink();
    private final TaskJuncture<Object, RuntimeException> juncture = (transaction, topic, junction, groups) -> Set.of();

    @ParameterizedTest(name = "consumer context first: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("#8 no child is committed for a parent that another context turned into a failure")
    void finding_8_children_follow_the_merged_decision(boolean consumerFirst) throws Exception {
        Task parent = new Task(1, "person-1");
        TaskConsumerContext<Object, RuntimeException, TaskSupplement> consumer =
            new TaskConsumerContext<>(Set.of(parent), sink, juncture, topic -> Optional.empty());
        consumer.pushByTask(parent, "child", TaskSink.Insertion.APPEND);
        TaskContext<Object, RuntimeException, TaskSupplement> veto = (decisions, executor, supplement) ->
            CompletableFuture.completedStage(transaction -> Map.of(parent, TaskDecision.FAILURE));
        CompositeTaskContext<Object, RuntimeException, TaskSupplement> composite = consumerFirst
            ? new CompositeTaskContext<>(consumer, veto)
            : new CompositeTaskContext<>(veto, consumer);

        TaskCompletion<Object, RuntimeException> completion = composite
            .apply(Map.of(parent, TaskDecision.SUCCESS), Runnable::run, new TaskSupplement())
            .toCompletableFuture()
            .get();
        Map<Task, TaskDecision> committed = completion.complete(null);

        assertThat(committed.get(parent).getResult()).isEqualTo(TaskResult.FAILURE);
        assertThat(sink.pushes).as("children written in the same completion as the failed parent").isEmpty();
    }

    @Test
    @DisplayName("#9 fan-out where one target topic gets no continued children")
    void finding_9_fan_out_with_an_empty_target_topic() throws Exception {
        Task kept = new Task(1, "person-1");
        Task dropped = new Task(2, "person-2");
        TaskConsumerContext<Object, RuntimeException, TaskSupplement> consumer =
            new TaskConsumerContext<>(Set.of(kept, dropped), sink, juncture, topic -> Optional.empty());
        consumer.pushByTask(kept, "first-target", TaskSink.Insertion.APPEND);
        consumer.pushByTask(dropped, "second-target", TaskSink.Insertion.APPEND);

        TaskCompletion<Object, RuntimeException> completion = consumer
            .apply(Map.of(kept, TaskDecision.SUCCESS, dropped, TaskDecision.FAILURE), Runnable::run, new TaskSupplement())
            .toCompletableFuture()
            .get();

        assertThatCode(() -> completion.complete(null)).doesNotThrowAnyException();
        assertThat(sink.pushes).containsExactly("first-target:person-1");
    }

    private static final class RecordingSink implements TaskSink<Object, RuntimeException> {

        private final List<String> pushes = new CopyOnWriteArrayList<>();
        private final AtomicLong sequence = new AtomicLong(100);

        @Override
        public List<Task> push(Object transaction, String topic, Insertion insertion, Collection<TaskCreation> creations) {
            List<Task> tasks = new ArrayList<>();
            for (TaskCreation creation : creations) {
                pushes.add(topic + ":" + creation.getIdentifier());
                tasks.add(new Task(sequence.incrementAndGet(), creation.getIdentifier()));
            }
            return tasks;
        }
    }
}
