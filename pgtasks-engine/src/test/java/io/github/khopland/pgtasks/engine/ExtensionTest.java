package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Extensions: installed when the engine is built, their jobs and lifecycle actions run with it. */
class ExtensionTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    /** Records what the engine asked of it. */
    private static final class Recording implements Extension {
        final List<String> events = new CopyOnWriteArrayList<>();
        final AtomicInteger leaderRuns = new AtomicInteger();
        final AtomicInteger nodeRuns = new AtomicInteger();
        ExtensionContext context;

        @Override
        public String name() {
            return "recording";
        }

        @Override
        public void install(ExtensionContext context) {
            this.context = context;
            events.add("installed");
            context.job(HousekeepingJob.leader("leader-count", Duration.ofHours(1), leaderRuns::incrementAndGet));
            context.job(HousekeepingJob.everyNode("node-count", Duration.ofMillis(20), nodeRuns::incrementAndGet));
            context.onStart(() -> events.add("started"));
            context.onStop(() -> events.add("stopped"));
        }
    }

    private static TaskEngineBuilder builder(InMemoryTaskStore store) {
        return TaskEngine.builder().store(store)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> Decision.success())));
    }

    @Test
    void an_extension_is_installed_once_and_its_lifecycle_follows_the_engine_through_a_restart() throws Exception {
        Recording extension = new Recording();
        TaskEngine engine = builder(new InMemoryTaskStore()).extension(extension).build();
        assertThat(extension.events).containsExactly("installed");
        assertThat(engine.extensions()).containsExactly("recording");

        engine.start();
        engine.stop(Duration.ofSeconds(5));
        engine.start();
        engine.stop(Duration.ofSeconds(5));

        assertThat(extension.events).containsExactly("installed", "started", "stopped", "started", "stopped");
    }

    @Test
    void every_node_jobs_run_on_their_own_and_leader_jobs_only_on_the_leader() throws Exception {
        Recording extension = new Recording();
        ManualClock clock = ManualClock.startingNow();
        InMemoryTaskStore store = new InMemoryTaskStore(clock);
        try (TaskEngine engine = builder(store).extension(extension).build().start()) {
            EngineFixture.await("the every-node job to run a few times", Duration.ofSeconds(5), () -> extension.nodeRuns.get() >= 3);
            EngineFixture.await("this node to lead", Duration.ofSeconds(5), engine::leadsHousekeeping);
            int before = extension.leaderRuns.get();

            TaskTester.of(engine).advance(Duration.ofMinutes(1));

            assertThat(extension.leaderRuns.get()).as("advance runs the leader's jobs, the extension's too").isGreaterThan(before);
            assertThat(extension.context.leading()).isTrue();
            assertThat(extension.context.clock().instant()).isEqualTo(clock.instant());
        }
    }

    @Test
    void a_message_on_an_extensions_channel_reaches_the_other_nodes_reloads() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        AtomicInteger reloads = new AtomicInteger();
        ExtensionContext[] sender = new ExtensionContext[1];
        Extension listening = context -> context.channel("settings", reloads::incrementAndGet);
        Extension sending = context -> sender[0] = context;
        try (TaskEngine ignored = builder(store).extension(listening).build().start();
             TaskEngine other = builder(store).extension(sending).build().start()) {
            int before = reloads.get();
            try (var transaction = sender[0].store().begin()) {
                sender[0].store().notify(transaction, "settings", "changed");
                transaction.commit();
            }
            EngineFixture.await("the reload on the other node", Duration.ofSeconds(5), () -> reloads.get() > before);
            assertThat(other.extensions()).hasSize(1);
        }
        assertThatThrownBy(() -> builder(store).extension(context -> context.channel("topic", () -> { })).build())
            .hasMessageContaining("Invalid channel 'topic'");
    }

    @Test
    void a_job_that_throws_runs_again() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        Extension failing = context -> context.job(HousekeepingJob.everyNode("failing", Duration.ofMillis(10), () -> {
            runs.incrementAndGet();
            throw new IllegalStateException("boom");
        }));
        try (TaskEngine ignored = builder(new InMemoryTaskStore()).extension(failing).build().start()) {
            EngineFixture.await("the failing job to run again", Duration.ofSeconds(5), () -> runs.get() >= 3);
        }
    }

    @Test
    void an_extension_that_fails_to_install_fails_the_build_and_one_comes_too_late_after_prepare() {
        Extension broken = context -> {
            throw new IllegalArgumentException("missing configuration");
        };
        assertThatThrownBy(() -> builder(new InMemoryTaskStore()).extension(broken).build())
            .hasMessageContaining("could not be installed").hasMessageContaining("missing configuration");

        TaskEngine engine = builder(new InMemoryTaskStore()).build().prepare();
        assertThatThrownBy(() -> engine.install(context -> { })).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> HousekeepingJob.leader("never", Duration.ZERO, () -> { })).hasMessageContaining("must be positive");
    }
}
