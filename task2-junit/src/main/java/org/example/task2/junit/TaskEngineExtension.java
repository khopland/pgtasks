package org.example.task2.junit;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import org.example.task2.Tasks;
import org.example.task2.engine.LocalTransactions;
import org.example.task2.engine.TaskAdmin;
import org.example.task2.engine.TaskConfigurer;
import org.example.task2.engine.TaskEngine;
import org.example.task2.engine.TaskEngineBuilder;
import org.example.task2.engine.TaskTester;
import org.example.task2.memory.InMemoryTaskStore;
import org.example.task2.spi.ManualClock;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * A task engine for each test, on the in-memory store with a {@link ManualClock}, started before
 * the test and stopped after it: handlers tested without a database or Spring, retries and delays
 * jumped through with {@link TaskTester#advance} or {@link TaskTester#drain}.
 *
 * <pre>{@code
 * @RegisterExtension
 * static TaskEngineExtension tasks = TaskEngineExtension.of(new NotifyTasks(new FakeMailer()));
 *
 * @Test
 * void sends_a_notice(Tasks tasks, TaskTester tester) {
 *     tasks.independently().push(NOTIFY, "anna", new Notice("hello"));
 *     tester.drain(NOTIFY);
 *     tester.assertPushed(ARCHIVE, 1);
 * }
 * }</pre>
 *
 * Test methods can take {@link TaskEngine}, {@link Tasks}, {@link TaskTester}, {@link TaskAdmin},
 * {@link ManualClock} and {@link LocalTransactions}, whose {@code inTransaction} gives
 * {@code tasks.push(...)} a transaction to join.
 */
public final class TaskEngineExtension implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

    private final Consumer<TaskEngineBuilder> setup;
    private ManualClock clock;
    private LocalTransactions transactions;
    private TaskEngine engine;
    private TaskTester tester;

    private TaskEngineExtension(Consumer<TaskEngineBuilder> setup) {
        this.setup = Objects.requireNonNull(setup, "setup");
    }

    /** An engine with these configurers, and defaults for everything else. */
    public static TaskEngineExtension of(TaskConfigurer... configurers) {
        return with(builder -> {
            for (TaskConfigurer configurer : configurers) {
                builder.configure(configurer);
            }
        });
    }

    /** An engine set up on the builder, which already has the store and transactions. */
    public static TaskEngineExtension with(Consumer<TaskEngineBuilder> setup) {
        return new TaskEngineExtension(setup);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        clock = ManualClock.startingNow();
        transactions = new LocalTransactions();
        TaskEngineBuilder builder = TaskEngine.builder().store(new InMemoryTaskStore(clock)).transactions(transactions).node("test");
        setup.accept(builder);
        engine = builder.build().start();
        tester = TaskTester.of(engine);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (engine != null) {
            engine.stop(Duration.ofSeconds(5));
            engine = null;
        }
    }

    public TaskEngine engine() {
        return started(engine);
    }

    public Tasks tasks() {
        return engine().tasks();
    }

    public TaskTester tester() {
        return started(tester);
    }

    public TaskAdmin admin() {
        return engine().admin();
    }

    public ManualClock clock() {
        return started(clock);
    }

    public LocalTransactions transactions() {
        return started(transactions);
    }

    private <T> T started(T value) {
        if (value == null) {
            throw new IllegalStateException("The engine runs only during a test: register the extension with @RegisterExtension");
        }
        return value;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameter, ExtensionContext extension) {
        Class<?> type = parameter.getParameter().getType();
        return type == TaskEngine.class || type == Tasks.class || type == TaskTester.class || type == TaskAdmin.class
            || type == ManualClock.class || type == LocalTransactions.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameter, ExtensionContext extension) {
        Class<?> type = parameter.getParameter().getType();
        if (type == TaskEngine.class) {
            return engine();
        } else if (type == Tasks.class) {
            return tasks();
        } else if (type == TaskTester.class) {
            return tester();
        } else if (type == TaskAdmin.class) {
            return admin();
        } else if (type == ManualClock.class) {
            return clock();
        }
        return transactions();
    }
}
