package org.example.task2.v1regressions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSink;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc.PostgresTaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskDispatcher;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskHandler;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskLimiter;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskListener;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskManager;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.PostgresExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Findings #1, #3, #4 and #5 (plan section 02), against the 1.x TaskManager. */
class TaskManagerRegressionTest {

    @RegisterExtension
    static PostgresExtension postgres = LegacySchema.extension();

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    @DisplayName("#1 TaskManager polls with its configured identifier condition")
    void finding_1_manager_applies_identifier_condition(PostgresDatabase database) throws Exception {
        PostgresTaskRepository repository = new PostgresTaskRepository(true, "node-a");
        try (Connection connection = database.connect()) {
            repository.register(connection);
            repository.initialize(connection, "topic");
            repository.push(connection, "topic", TaskSink.Insertion.APPEND, "person-1");
            repository.push(connection, "topic", TaskSink.Insertion.APPEND, "person-1");
            repository.push(connection, "topic", TaskSink.Insertion.APPEND, "person-2");
        }
        List<List<String>> batches = new CopyOnWriteArrayList<>();
        CountDownLatch completed = new CountDownLatch(3);
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executor,
            repository,
            LegacySchema.dispatcher(database.dataSource()),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> {
                batches.add(tasks.stream().sorted().map(Task::getIdentifier).toList());
                callback.accept(transaction -> tasks.stream().collect(Collectors.toMap(Function.identity(), task -> TaskDecision.SUCCESS)));
                tasks.forEach(task -> completed.countDown());
            },
            TaskListener.noop(),
            LegacySchema.fifo(),
            TaskSource.Condition.SINGULAR_BY_IDENTIFIER,
            10,
            100,
            TimeUnit.MILLISECONDS
        );
        assertThat(processor.start(1, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(completed.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            processor.stop(1, TimeUnit.SECONDS);
        }
        assertThat(batches)
            .as("no batch may hold two tasks for the same identifier under SINGULAR_BY_IDENTIFIER")
            .allSatisfy(batch -> assertThat(batch).doesNotHaveDuplicates());
    }

    @Test
    @DisplayName("#3 a failing poll backs off instead of spinning")
    void finding_3_poll_failure_backs_off() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        TaskSource<Object, RuntimeException> failing = new TaskSource<>() {
            @Override
            public Set<Task> poll(Object transaction, String topic, Order order, Condition condition, int size) {
                polls.incrementAndGet();
                throw new IllegalStateException("database is down");
            }

            @Override
            public void complete(Object transaction, String topic, Map<Task, TaskDecision> decisions) {
            }
        };
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executor,
            failing,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> TaskHandler.simple(task -> TaskDecision.SUCCESS),
            TaskListener.noop(),
            LegacySchema.fifo(),
            TaskSource.Condition.NONE,
            10,
            1,
            TimeUnit.SECONDS
        );
        assertThat(processor.start(1, TimeUnit.SECONDS)).isTrue();
        try {
            Thread.sleep(500);
        } finally {
            processor.stop(1, TimeUnit.SECONDS);
        }
        assertThat(polls.get())
            .as("polls during 500 ms of failures with a 1 s pause")
            .isLessThanOrEqualTo(5);
    }

    @Test
    @DisplayName("#4 start() reports a timeout when the executor has not started the loop")
    void finding_4_start_timeout_is_a_timeout() {
        AtomicReference<Runnable> queued = new AtomicReference<>();
        TaskProcessor processor = idleManager(queued::set, new AtomicInteger());

        assertThatThrownBy(() -> processor.start(10, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    @Test
    @DisplayName("#4 a loop queued before a start() timeout never runs")
    void finding_4_timed_out_start_leaves_no_orphaned_loop() throws Exception {
        AtomicReference<Runnable> queued = new AtomicReference<>();
        AtomicInteger polls = new AtomicInteger();
        TaskProcessor processor = idleManager(queued::set, polls);
        try {
            processor.start(10, TimeUnit.MILLISECONDS);
        } catch (Exception expected) {
            // a timeout, or the NPE from finding #4
        }

        Thread late = Thread.ofVirtual().start(queued.get());
        try {
            Thread.sleep(300);
            assertThat(polls.get()).as("polls by a loop whose start() already failed").isZero();
            assertThat(processor.isActive()).isFalse();
        } finally {
            processor.stop(1, TimeUnit.SECONDS);
            late.join(1000);
        }
    }

    @Test
    @DisplayName("#5 the manager claims with Postgres' single round-trip poll")
    void finding_5_manager_uses_single_round_trip_poll(PostgresDatabase database) throws Exception {
        PostgresTaskRepository repository = new PostgresTaskRepository(true, "node-a");
        try (Connection connection = database.connect()) {
            repository.register(connection);
            repository.initialize(connection, "topic");
            repository.push(connection, "topic", TaskSink.Insertion.APPEND, "person-1", "person-2");
        }
        List<Integer> statementsPerPoll = new CopyOnWriteArrayList<>();
        CountDownLatch completed = new CountDownLatch(2);
        TaskProcessor processor = new TaskManager<>(
            "topic",
            executor,
            repository,
            countingDispatcher(database.dataSource(), statementsPerPoll),
            TaskLimiter.noop(),
            topic -> (tasks, callback, onFailure) -> {
                callback.accept(transaction -> tasks.stream().collect(Collectors.toMap(Function.identity(), task -> TaskDecision.SUCCESS)));
                tasks.forEach(task -> completed.countDown());
            },
            TaskListener.noop(),
            LegacySchema.fifo(),
            TaskSource.Condition.NONE,
            10,
            100,
            TimeUnit.MILLISECONDS
        );
        assertThat(processor.start(1, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(completed.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            processor.stop(1, TimeUnit.SECONDS);
        }
        assertThat(statementsPerPoll).as("statements in the claiming transaction").first().isEqualTo(1);
    }

    private TaskProcessor idleManager(java.util.concurrent.Executor executor, AtomicInteger polls) {
        TaskSource<Object, RuntimeException> idle = new TaskSource<>() {
            @Override
            public Set<Task> poll(Object transaction, String topic, Order order, Condition condition, int size) {
                polls.incrementAndGet();
                return Set.of();
            }

            @Override
            public void complete(Object transaction, String topic, Map<Task, TaskDecision> decisions) {
            }
        };
        return new TaskManager<>(
            "topic",
            executor,
            idle,
            TaskDispatcher.simple(() -> null),
            TaskLimiter.noop(),
            topic -> TaskHandler.simple(task -> TaskDecision.SUCCESS),
            TaskListener.noop(),
            LegacySchema.fifo(),
            TaskSource.Condition.NONE,
            10,
            50,
            TimeUnit.MILLISECONDS
        );
    }

    /** Like {@link LegacySchema#dispatcher}, but records how many statements each claiming transaction prepared. */
    private static TaskDispatcher<Connection, SQLException> countingDispatcher(DataSource dataSource, List<Integer> statementsPerPoll) {
        TaskDispatcher<Connection, SQLException> delegate = LegacySchema.dispatcher(dataSource);
        return new TaskDispatcher<>() {
            @Override
            public <P> P apply(TransactionFunction<Connection, SQLException, P> function) throws SQLException {
                return delegate.apply(connection -> {
                    AtomicInteger statements = new AtomicInteger();
                    P payload = function.apply(counting(connection, statements));
                    if (payload instanceof Set<?> polled && !polled.isEmpty()) {
                        statementsPerPoll.add(statements.get());
                    }
                    return payload;
                });
            }
        };
    }

    private static Connection counting(Connection connection, AtomicInteger statements) {
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, arguments) -> {
                if (method.getName().equals("prepareStatement") || method.getName().equals("createStatement") || method.getName().equals("prepareCall")) {
                    statements.incrementAndGet();
                }
                try {
                    return method.invoke(connection, arguments);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );
    }
}
