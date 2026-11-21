package io.github.khopland.task2.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.task2.Decision;
import io.github.khopland.task2.TopicRef;
import io.github.khopland.task2.memory.InMemoryTaskStore;
import io.github.khopland.task2.spi.ManualClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Dead letters that come back by themselves: on a new application version, or every so often. */
class DeadLetterPolicyTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private final ManualClock clock = ManualClock.startingNow();
    private final InMemoryTaskStore store = new InMemoryTaskStore(clock);
    private final AtomicBoolean fixed = new AtomicBoolean();
    private final AtomicInteger runs = new AtomicInteger();
    private final List<TaskEngine> engines = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        engines.forEach(TaskEngine::close);
    }

    private TaskEngine start(String version, DeadLetterPolicy policy) throws Exception {
        TaskEngine engine = TaskEngine.builder()
            .store(store)
            .version(version)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                return fixed.get() ? Decision.success() : Decision.failed("bug");
            }), topic -> topic.retry(Retry.none()).deadLetters(policy)))
            .build()
            .start();
        engines.add(engine);
        return engine;
    }

    @Test
    @DisplayName("The first node of a new version revives the dead letters once")
    void a_new_version_revives_the_dead_letters_once() throws Exception {
        DeadLetterPolicy policy = DeadLetterPolicy.KEEP.withReviveOnNewVersion();
        TaskEngine first = start("1.0", policy);
        long id = first.tasks().independently().push(WORK, "a", "payload").id();
        TaskTester.of(first).awaitIdle();
        assertThat(first.admin().find(id).orElseThrow().state()).isEqualTo("failed");
        first.close();

        TaskEngine restarted = start("1.0", policy);
        TaskTester.of(restarted).awaitIdle();
        assertThat(restarted.admin().find(id).orElseThrow().state()).as("the same version doesn't revive again").isEqualTo("failed");
        restarted.close();

        fixed.set(true);
        TaskEngine upgraded = start("1.1", policy);
        TaskEngine sameVersion = start("1.1", policy);
        TaskTester.of(upgraded, sameVersion).awaitIdle();

        assertThat(upgraded.admin().find(id).orElseThrow().state()).isEqualTo("succeeded");
        assertThat(runs).as("one failure, one run after the revival").hasValue(2);
    }

    @Test
    @DisplayName("A sweep revives the dead letters every so often, like the 1.x reset job")
    void a_sweep_revives_them_every_so_often() throws Exception {
        TaskEngine engine = start("1.0", DeadLetterPolicy.KEEP.withReviveEvery(Duration.ofHours(6)));
        TaskTester tester = TaskTester.of(engine);
        long id = engine.tasks().independently().push(WORK, "a", "payload").id();
        tester.awaitIdle();
        assertThat(engine.admin().find(id).orElseThrow().state()).isEqualTo("failed");

        tester.advance(Duration.ofHours(1));
        tester.awaitIdle();
        assertThat(engine.admin().find(id).orElseThrow().state()).as("not yet").isEqualTo("failed");

        fixed.set(true);
        tester.advance(Duration.ofHours(6));
        tester.awaitIdle();
        assertThat(engine.admin().find(id).orElseThrow().state()).isEqualTo("succeeded");
    }
}
