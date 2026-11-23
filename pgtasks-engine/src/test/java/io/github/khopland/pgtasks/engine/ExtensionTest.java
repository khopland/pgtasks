package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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

    private static final SettingKey<Integer> WINDOW = SettingKey.of("window.size", Integer.class, 10, Integer::valueOf);

    /** Declares a setting and keeps its context, to read it back. */
    private static final class Windowed implements Extension {
        ExtensionContext context;

        @Override
        public List<SettingKey<?>> settings() {
            return List.of(WINDOW);
        }

        @Override
        public void install(ExtensionContext context) {
            this.context = context;
        }
    }

    @Test
    void an_extensions_setting_layers_like_the_engines_own_and_shows_where_it_came_from() {
        TopicRef<String> plain = TopicRef.of("plain", String.class), moduled = TopicRef.of("moduled", String.class),
            own = TopicRef.of("own", String.class), overridden = TopicRef.of("overridden", String.class);
        Windowed extension = new Windowed();
        TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore()).extension(extension)
            .configure("a", tasks -> tasks.eachTask(plain, task -> { }))
            .configure("b", tasks -> tasks
                .defaults(options -> options.set(WINDOW, 20))
                .eachTask(moduled, task -> { })
                .eachTask(own, task -> { }, options -> options.set(WINDOW, 30))
                .eachTask(overridden, task -> { }))
            .overrides(Map.of("topics.overridden.window.size", "40"))
            .build();

        assertThat(extension.context.topicSetting(plain, WINDOW)).hasValue(10);
        assertThat(extension.context.topicSetting(moduled, WINDOW)).hasValue(20);
        assertThat(extension.context.topicSetting(own, WINDOW)).hasValue(30);
        assertThat(extension.context.topicSetting(overridden, WINDOW)).hasValue(40);
        assertThat(extension.context.topicSetting(TopicRef.of("elsewhere", String.class), WINDOW)).isEmpty();
        assertThat(engine.effectiveSettings()).filteredOn(settings -> settings.topic().equals("overridden")).singleElement()
            .satisfies(settings -> assertThat(settings.get("window.size"))
                .isEqualTo(new EffectiveSettings.Setting("40", EffectiveSettings.Source.OVERRIDE)));
        assertThat(engine.effectiveSettings()).filteredOn(settings -> settings.topic().equals("plain")).singleElement()
            .satisfies(settings -> assertThat(settings.get("window.size").source()).isEqualTo(EffectiveSettings.Source.DEFAULT));

        assertThatThrownBy(() -> TaskEngine.builder().store(new InMemoryTaskStore()).extension(new Windowed())
            .configure("a", tasks -> tasks.eachTask(plain, task -> { }))
            .overrides(Map.of("topics.plain.window.size", "wide", "topics.plain.window.other", "1"))
            .build())
            .hasMessageContaining("topics.plain.window.size").hasMessageContaining("topics.plain.window.other: unknown setting");
        assertThatThrownBy(() -> SettingKey.of("nodot", Integer.class, 1, Integer::valueOf)).hasMessageContaining("dotted");
    }

    @Test
    void a_rate_limit_built_on_the_hooks_alone_holds_its_topic_back_and_a_listener_sees_every_result() throws Exception {
        TopicRef<String> limited = TopicRef.of("limited", String.class);
        List<BatchListener.Completed> seen = new CopyOnWriteArrayList<>();
        Extension listening = context -> context.batchListener((topic, tasks) -> seen.addAll(tasks));
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .extension(new NodeRateLimit(), listening)
            .configure("work", tasks -> tasks.eachTask(limited, task -> { }, options -> options.set(NodeRateLimit.RATE, 20.0)))
            .build().start()) {
            long started = System.nanoTime();
            for (int index = 0; index < 30; index++) {
                engine.tasks().independently().push(limited, "t" + index, "x");
            }
            EngineFixture.await("every task to finish", Duration.ofSeconds(10), () -> seen.size() == 30);
            Duration took = Duration.ofNanos(System.nanoTime() - started);

            assertThat(took).as("30 tasks at 20/s with a burst of 20 take at least half a second").isGreaterThan(Duration.ofMillis(450));
            assertThat(seen).extracting(BatchListener.Completed::result).containsOnly(TaskObserver.Result.SUCCEEDED);
            assertThat(seen).extracting(BatchListener.Completed::identifier).doesNotHaveDuplicates();
        }
        assertThatThrownBy(() -> ClaimGate.Permit.atMost(0)).isInstanceOf(IllegalArgumentException.class);
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
