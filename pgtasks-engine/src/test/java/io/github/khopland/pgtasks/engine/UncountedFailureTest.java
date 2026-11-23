package io.github.khopland.pgtasks.engine;

import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Failures that aren't the task's fault don't use up its attempts; some failures dead-letter at once. */
class UncountedFailureTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private final ManualClock clock = ManualClock.startingNow();
    private TaskEngine engine;
    private TaskTester tester;

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.close();
        }
    }

    private long run(Retry retry, BatchHandler<String> handler) throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .configure("work", tasks -> tasks.topic(WORK, handler, topic -> topic.retry(retry)))
            .build()
            .start();
        tester = TaskTester.of(engine);
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        tester.drain(WORK);
        return id;
    }

    @Test
    @DisplayName("An outage longer than the attempts allow costs no attempts: the task succeeds afterwards")
    void uncounted_failures_dont_use_up_attempts() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        long id = run(Retry.standard().attempts(3).notCounting(ConnectException.class), batch -> batch.decide(task ->
            runs.incrementAndGet() <= 10 ? Decision.failed(new ConnectException("downstream is down")) : Decision.success()));

        TaskView done = engine.admin().find(id).orElseThrow();
        assertThat(runs).hasValue(11);
        assertThat(done.state()).isEqualTo("succeeded");
        assertThat(done.attempt()).as("ten uncounted failures, one counted success").isEqualTo(1);
    }

    @Test
    @DisplayName("Backoff grows with uncounted failures too, so an outage isn't hammered")
    void the_backoff_grows_through_uncounted_failures() throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> Decision.failed(new ConnectException("down"))),
                topic -> topic.retry(Retry.standard().notCounting(ConnectException.class))))
            .build()
            .start();
        tester = TaskTester.of(engine);
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        tester.awaitIdle();
        for (int failure = 1; failure <= 4; failure++) {
            TaskView view = engine.admin().find(id).orElseThrow();
            assertThat(view.failures()).isEqualTo(failure);
            Duration backoff = Duration.between(clock.instant(), view.runAt());
            // 10 s doubling, up to 20 % shorter.
            assertThat(backoff).isBetween(Duration.ofSeconds(10L << (failure - 1)).multipliedBy(8).dividedBy(10),
                Duration.ofSeconds(10L << (failure - 1)));
            tester.advance(backoff);
            tester.awaitIdle();
        }
        assertThat(engine.admin().find(id).orElseThrow().error()).contains("down");
    }

    @Test
    @DisplayName("Types named by the policy: not counted, or straight to the dead letters, anywhere in the cause chain")
    void the_policy_classifies_failures_by_cause() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        Retry retry = Retry.standard().attempts(2).notCounting(ConnectException.class).deadLetterAt(IllegalArgumentException.class);
        long id = run(retry, batch -> batch.decide(task -> {
            if (runs.incrementAndGet() <= 5) {
                return Decision.failed(new IllegalStateException("call failed", new ConnectException("refused")));
            }
            return Decision.failed(new IllegalArgumentException("bad payload"));
        }));

        TaskView dead = engine.admin().find(id).orElseThrow();
        assertThat(runs).as("five uncounted failures, then a dead letter at once").hasValue(6);
        assertThat(dead.state()).isEqualTo("failed");
        assertThat(dead.attempt()).isEqualTo(1);
        assertThat(dead.error()).contains("bad payload");
    }

    @Test
    @DisplayName("Under Retry.none(), an uncounted failure still backs off rather than spinning")
    void no_retries_still_back_off_when_not_counted() throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> Decision.failed(new ConnectException("down"))),
                topic -> topic.retry(Retry.none().notCounting(ConnectException.class))))
            .build()
            .start();
        tester = TaskTester.of(engine);
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        tester.awaitIdle();

        TaskView view = engine.admin().find(id).orElseThrow();
        assertThat(view.state()).isEqualTo("retrying");
        assertThat(Duration.between(clock.instant(), view.runAt())).isGreaterThanOrEqualTo(Duration.ofSeconds(8));
    }
}
