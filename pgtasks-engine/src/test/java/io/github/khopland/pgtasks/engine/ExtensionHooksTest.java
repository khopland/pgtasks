package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.ExtensionData;
import io.github.khopland.pgtasks.engine.extension.HousekeepingJob;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.engine.extension.SettingKey;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ExtensionEntry;
import io.github.khopland.pgtasks.spi.ManualClock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** The hooks extensions are built on, each as an extension uses it: pause, breaker, recurring tasks, rate limits. */
class ExtensionHooksTest {

    private static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    private static TaskEngineBuilder builder(InMemoryTaskStore store) {
        return TaskEngine.builder().store(store)
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> Decision.success())));
    }

    /** Keeps the context it was installed with. */
    private static final class Kept implements Extension {
        private final String name;
        ExtensionContext context;

        Kept(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void install(ExtensionContext context) {
            this.context = context;
        }
    }

    @Test
    @DisplayName("Start actions run before topics claim and jobs run, and one that throws fails the start with nothing claimed")
    void start_actions_run_before_topics_and_jobs() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        List<String> events = new CopyOnWriteArrayList<>();
        Extension ordered = Extension.of("ordered", context -> {
            context.onStart(() -> events.add("start"));
            context.claimGate((topic, wanted) -> {
                events.add("gate");
                return ClaimGate.Permit.ANY;
            });
            context.job(HousekeepingJob.everyNode("ordered job", Duration.ofMillis(10), () -> events.add("job")));
        });
        try (TaskEngine engine = builder(store).extension(ordered).build().prepare()) {
            engine.tasks().independently().push(WORK, "a", "x");
            engine.start();
            TaskTester.of(engine).awaitIdle();
            assertThat(events).first().isEqualTo("start");
            assertThat(events).contains("gate", "job");
        }

        AtomicInteger handled = new AtomicInteger();
        List<String> stopped = new CopyOnWriteArrayList<>();
        Extension first = Extension.of("first", context -> context.onStop(() -> stopped.add("first")));
        Extension failing = Extension.of("failing", context -> {
            context.onStart(() -> {
                throw new IllegalStateException("its table is missing");
            });
            context.onStop(() -> stopped.add("failing"));
        });
        TaskEngine engine = TaskEngine.builder().store(store).extension(first, failing)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> handled.incrementAndGet()))
            .build();
        assertThatThrownBy(engine::start).hasMessageContaining("its table is missing");
        assertThat(engine.running()).isFalse();
        assertThat(handled).as("nothing was claimed").hasValue(0);
        assertThat(stopped).as("the extension that started stops, the one that failed to doesn't").containsExactly("first");
        assertThat(engine.admin().nodes()).as("the node that failed to start is gone").isEmpty();

        Extension checked = Extension.of("checked", context -> context.onStart(() -> {
            throw new java.sql.SQLException("relation \"window\" does not exist");
        }));
        assertThatThrownBy(() -> builder(store).extension(checked).build().start())
            .isInstanceOf(io.github.khopland.pgtasks.TaskStorageException.class).hasMessageContaining("relation \"window\" does not exist");
    }

    @Test
    @DisplayName("Stop actions run once the topics and jobs are done with, the last extension installed first")
    void stop_actions_run_last_and_in_reverse() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch handling = new CountDownLatch(1);
        Extension first = Extension.of("first", context -> {
            context.batchListener((topic, tasks) -> events.add("batch done"));
            context.onStop(() -> events.add("first stops"));
        });
        Extension second = Extension.of("second", context -> {
            context.onStop(() -> events.add("second stops, its first action"));
            context.onStop(() -> events.add("second stops"));
        });
        TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore()).extension(first, second)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> {
                handling.countDown();
                LockSupport.parkNanos(Duration.ofMillis(300).toNanos());
            }))
            .build().start();
        engine.tasks().independently().push(WORK, "a", "x");
        assertThat(handling.await(5, TimeUnit.SECONDS)).isTrue();
        engine.stop(Duration.ofSeconds(5));

        assertThat(events).containsExactly("batch done", "second stops", "second stops, its first action", "first stops");
    }

    @Test
    @DisplayName("A channel's reload waits for the start actions, and is told what arrived meanwhile")
    void a_channel_reload_waits_for_the_start_actions() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        ExtensionContext.Channel[] channel = new ExtensionContext.Channel[1];
        Extension listening = Extension.of("listening", context -> {
            channel[0] = context.channel("pause", payloads -> events.add("reload " + payloads));
            context.onStart(() -> {
                // The engine listens already, so this node gets its own message while it starts.
                context.transact("Telling", transaction -> {
                    channel[0].send(transaction, "orders");
                    return null;
                });
                LockSupport.parkNanos(Duration.ofMillis(300).toNanos());
                events.add("started");
            });
        });
        try (TaskEngine engine = builder(new InMemoryTaskStore()).extension(listening).build().start()) {
            EngineFixture.await("a reload", Duration.ofSeconds(5), () -> events.size() > 1);
            assertThat(events).first().as("no reload before the start action returned").isEqualTo("started");
            assertThat(events.get(1)).as("the message kept, or a reload of everything").isIn("reload [orders]", "reload []");
            assertThat(channel[0].name()).isEqualTo("pause");
            assertThat(engine.running()).isTrue();
        }
    }

    @Test
    @DisplayName("A leader job runs under a lease of its own, on a node that has it, whoever runs the housekeeping")
    void a_leader_job_has_a_lease_of_its_own() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        AtomicInteger runs = new AtomicInteger();
        ExtensionContext.Job[] cleanupJob = new ExtensionContext.Job[1];
        Extension cleanup = Extension.of("cleanup", context ->
            cleanupJob[0] = context.job(HousekeepingJob.leader("cleanup", Duration.ofMillis(10), runs::incrementAndGet)));
        try (TaskEngine plain = builder(store).node("plain").build().start()) {
            EngineFixture.await("the plain node to lead housekeeping", Duration.ofSeconds(5), plain::leadsHousekeeping);
            try (TaskEngine extended = builder(store).node("extended").extension(cleanup).build().start()) {
                EngineFixture.await("the leader job to run", Duration.ofSeconds(5), () -> runs.get() > 0);

                assertThat(plain.leadsHousekeeping()).as("the housekeeping lease stays where it was").isTrue();
                assertThat(cleanupJob[0].runsHere()).isTrue();
                assertThat(cleanupJob[0].name()).isEqualTo("cleanup");
                assertThat(plain.admin().nodes()).filteredOn(node -> node.name().equals("extended")).singleElement()
                    .satisfies(node -> assertThat(node.leases()).as("its job's lease, named after its extension, not the housekeeping lease")
                        .containsExactly("cleanup/cleanup"));
                assertThat(extended.health().jobs()).filteredOn(job -> job.name().equals("cleanup")).singleElement()
                    .satisfies(job -> {
                        assertThat(job.runsHere()).isTrue();
                        assertThat(job.lastRun()).isNotNull();
                        assertThat(job.failing()).isFalse();
                    });
            }
        }

        assertThatThrownBy(() -> builder(new InMemoryTaskStore())
            .extension(Extension.of("x", context -> context.job(HousekeepingJob.leader("promotion", Duration.ofSeconds(1), () -> { }))))
            .build()).hasMessageContaining("job named 'promotion' of its own");
        assertThatThrownBy(() -> builder(new InMemoryTaskStore())
            .extension(Extension.of("x", context -> {
                context.job(HousekeepingJob.everyNode("twice", Duration.ofSeconds(1), () -> { }));
                context.job(HousekeepingJob.leader("twice", Duration.ofSeconds(1), () -> { }));
            }))
            .build()).hasMessageContaining("Another job is named 'twice'");
    }

    @Test
    void a_job_asked_to_run_soon_runs_before_its_interval() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        ExtensionContext.Job[] job = new ExtensionContext.Job[1];
        Extension slow = Extension.of("slow", context -> job[0] = context.job(HousekeepingJob.everyNode("slow", Duration.ofHours(1), runs::incrementAndGet)));
        try (TaskEngine engine = builder(new InMemoryTaskStore()).extension(slow).build().start()) {
            EngineFixture.await("the first run", Duration.ofSeconds(5), () -> runs.get() == 1);
            assertThat(job[0].runsHere()).isTrue();
            job[0].runSoon();
            EngineFixture.await("the run asked for", Duration.ofSeconds(5), () -> runs.get() == 2);
            assertThat(engine.running()).isTrue();
        }
        assertThat(job[0].runsHere()).as("not once stopped").isFalse();
        job[0].runSoon();
        assertThat(runs).as("nothing once stopped").hasValue(2);
    }

    @Test
    @DisplayName("A leader job asked to run soon on a node without its lease runs on the node that holds it")
    void run_soon_reaches_the_node_that_holds_the_lease() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        Map<String, ExtensionContext.Job> jobs = new ConcurrentHashMap<>();
        java.util.function.Function<String, Extension> sweeper = node -> Extension.of("sweeper", context -> jobs.put(node,
            context.job(HousekeepingJob.leader("sweep", Duration.ofHours(1), () -> runs.computeIfAbsent(node, ignored -> new AtomicInteger()).incrementAndGet()))));
        try (TaskEngine a = builder(store).node("a").extension(sweeper.apply("a")).build().start()) {
            EngineFixture.await("a to take the job's lease", Duration.ofSeconds(5), () -> jobs.get("a").runsHere());
            try (TaskEngine b = builder(store).node("b").extension(sweeper.apply("b")).build().start()) {
                assertThat(jobs.get("b").runsHere()).isFalse();
                int before = runs.computeIfAbsent("a", ignored -> new AtomicInteger()).get();

                jobs.get("b").runSoon();
                EngineFixture.await("the run on the node holding the lease", Duration.ofSeconds(5), () -> runs.get("a").get() == before + 1);
                jobs.get("a").runSoon();
                EngineFixture.await("a run asked for where it runs", Duration.ofSeconds(5), () -> runs.get("a").get() == before + 2);
                assertThat(runs.get("b")).as("never on the node without the lease").isNull();
                assertThat(a.running() && b.running()).isTrue();
            }
        }
    }

    @Test
    @DisplayName("Leader jobs of one name in two extensions have a lease each, so both run")
    void leader_jobs_of_one_name_in_two_extensions_both_run() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        AtomicInteger audits = new AtomicInteger();
        AtomicInteger archives = new AtomicInteger();
        try (TaskEngine auditing = builder(store).node("auditing")
                 .extension(Extension.of("audit", context -> context.job(HousekeepingJob.leader("cleanup", Duration.ofMillis(10), audits::incrementAndGet))))
                 .build().start();
             TaskEngine archiving = builder(store).node("archiving")
                 .extension(Extension.of("archive", context -> context.job(HousekeepingJob.leader("cleanup", Duration.ofMillis(10), archives::incrementAndGet))))
                 .build().start()) {
            EngineFixture.await("both jobs to run", Duration.ofSeconds(5), () -> audits.get() > 0 && archives.get() > 0);
            assertThat(auditing.admin().nodes()).flatExtracting(node -> node.leases()).contains("audit/cleanup", "archive/cleanup");
            assertThat(archiving.running()).isTrue();
        }
    }

    @Test
    void the_health_shows_each_jobs_failures_until_one_run_succeeds() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        Extension flaky = Extension.of("flaky", context -> context.job(HousekeepingJob.everyNode("flaky", Duration.ofMillis(10), () -> {
            if (runs.incrementAndGet() <= 3) {
                throw new IllegalStateException("boom " + runs.get());
            }
        })));
        try (TaskEngine engine = builder(new InMemoryTaskStore()).extension(flaky).build().start()) {
            EngineFixture.await("a run to succeed", Duration.ofSeconds(5), () -> runs.get() > 4);
            EngineHealth.JobStatus job = engine.health().jobs().stream().filter(each -> each.name().equals("flaky")).findFirst().orElseThrow();

            assertThat(job.failing()).isFalse();
            assertThat(job.lastFailure()).isNotNull();
            assertThat(job.lastError()).contains("boom 3");
            assertThat(engine.health().jobs()).extracting(EngineHealth.JobStatus::name)
                .contains("orphan sweep", "reconcile", "expiry", "promotion", "retention", "count snapshot", "flaky");
        }
    }

    private static final SettingKey<Integer> SIZE = SettingKey.of("window.size", Integer.class, 10, Integer::valueOf)
        .checking(size -> {
            if (size < 1) {
                throw new IllegalArgumentException("at least 1, not " + size);
            }
        });

    @Test
    @DisplayName("A setting's check applies to its default, to values set in code and to overrides")
    void a_settings_check_applies_to_every_value() {
        Extension sized = new Extension() {
            @Override
            public List<SettingKey<?>> settings() {
                return List.of(SIZE);
            }

            @Override
            public void install(ExtensionContext context) {
            }
        };
        assertThatThrownBy(() -> TaskEngine.builder().store(new InMemoryTaskStore()).extension(sized)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { }, options -> options.set(SIZE, 0)))
            .build()).hasMessageContaining("window.size: at least 1, not 0");
        assertThatThrownBy(() -> TaskEngine.builder().store(new InMemoryTaskStore()).extension(sized)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { }))
            .overrides(Map.of("topics.work.window.size", "-3"))
            .build()).hasMessageContaining("topics.work.window.size: at least 1, not -3");
        assertThatThrownBy(() -> SettingKey.of("window.size", Integer.class, 0, Integer::valueOf).checking(size -> {
            if (size < 1) {
                throw new IllegalArgumentException("at least 1");
            }
        })).hasMessageContaining("The default of window.size is invalid");
        assertThat(SIZE.parse(" 7 ")).isEqualTo(7);

        SettingKey<Integer> unchecked = SettingKey.of("window.size", Integer.class, 10, Integer::valueOf);
        assertThat(unchecked).as("one setting, whatever its checks").isEqualTo(SIZE).hasSameHashCodeAs(SIZE);
        assertThatThrownBy(() -> TaskEngine.builder().store(new InMemoryTaskStore()).extension(sized)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { }, options -> options.set(unchecked, 0)))
            .build()).as("the declared key's check, though set with another").hasMessageContaining("window.size: at least 1, not 0");
        SettingKey<Integer> bounded = SIZE.checking(size -> {
            if (size > 100) {
                throw new IllegalArgumentException("at most 100, not " + size);
            }
        });
        assertThatThrownBy(() -> bounded.parse("0")).hasMessageContaining("at least 1");
        assertThatThrownBy(() -> bounded.parse("101")).hasMessageContaining("at most 100");
    }

    private static final SettingKey<Duration> EVERY = SettingKey.of("audit.every", Duration.class, Duration.ofMinutes(1), Duration::parse)
        .checking(every -> {
            if (!every.isPositive()) {
                throw new IllegalArgumentException("must be positive");
            }
        });

    /** Declares an engine-wide setting and keeps its context. */
    private static final class Audit implements Extension {
        ExtensionContext context;

        @Override
        public List<SettingKey<?>> engineSettings() {
            return List.of(EVERY);
        }

        @Override
        public void install(ExtensionContext context) {
            this.context = context;
        }
    }

    @Test
    void an_engine_wide_setting_takes_the_builders_value_and_an_override_and_shows_where_each_came_from() {
        Audit plain = new Audit();
        TaskEngine engine = builder(new InMemoryTaskStore()).extension(plain).build();
        assertThat(plain.context.setting(EVERY)).isEqualTo(Duration.ofMinutes(1));
        assertThat(engine.engineSettings().get("audit.every"))
            .isEqualTo(new EffectiveSettings.Setting("PT1M", EffectiveSettings.Source.DEFAULT));

        Audit set = new Audit();
        TaskEngine coded = builder(new InMemoryTaskStore()).extension(set).set(EVERY, Duration.ofMinutes(2)).build();
        assertThat(set.context.setting(EVERY)).isEqualTo(Duration.ofMinutes(2));
        assertThat(coded.engineSettings().get("audit.every").source()).isEqualTo(EffectiveSettings.Source.CODE);

        Audit overridden = new Audit();
        TaskEngine overriding = builder(new InMemoryTaskStore()).extension(overridden).set(EVERY, Duration.ofMinutes(2))
            .overrides(Map.of("extensions.audit.every", "PT3M")).build();
        assertThat(overridden.context.setting(EVERY)).isEqualTo(Duration.ofMinutes(3));
        assertThat(overriding.engineSettings().get("audit.every").source()).isEqualTo(EffectiveSettings.Source.OVERRIDE);

        assertThatThrownBy(() -> plain.context.setting(SIZE)).hasMessageContaining("No installed extension declares the engine setting window.size");
        assertThatThrownBy(() -> builder(new InMemoryTaskStore()).extension(new Audit())
            .overrides(Map.of("extensions.audit.every", "PT0S", "extensions.audit.other", "1")).build())
            .hasMessageContaining("extensions.audit.every: must be positive")
            .hasMessageContaining("extensions.audit.other: unknown setting");
        assertThatThrownBy(() -> builder(new InMemoryTaskStore()).set(EVERY, Duration.ofMinutes(2)).build())
            .hasMessageContaining("no installed extension declares");
        assertThatThrownBy(() -> TaskEngine.builder().set(EVERY, Duration.ZERO)).hasMessageContaining("audit.every: must be positive");
        SettingKey<Duration> uncheckedEvery = SettingKey.of("audit.every", Duration.class, Duration.ofMinutes(1), Duration::parse);
        assertThatThrownBy(() -> builder(new InMemoryTaskStore()).extension(new Audit()).set(uncheckedEvery, Duration.ZERO).build())
            .as("the declared key's check, though set with another").hasMessageContaining("audit.every: must be positive");
    }

    @Test
    @DisplayName("Hooks are taken while an extension installs, and one name is installed once")
    void hooks_only_while_installing_and_one_extension_per_name() {
        Kept kept = new Kept("kept");
        TaskEngine engine = builder(new InMemoryTaskStore()).extension(kept).build();

        assertThatThrownBy(() -> kept.context.claimGate((topic, wanted) -> ClaimGate.Permit.ANY))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("kept registered a claim gate after it was installed");
        assertThatThrownBy(() -> kept.context.onStart(() -> { })).isInstanceOf(IllegalStateException.class);
        assertThat(kept.context.store()).as("the rest of the context still works").isNotNull();
        assertThat(engine.extension(Kept.class)).containsSame(kept);
        assertThat(engine.extension(Audit.class)).isEmpty();

        assertThatThrownBy(() -> builder(new InMemoryTaskStore()).extension(new Kept("twice"), new Kept("twice")).build())
            .hasMessageContaining("An extension named twice is installed already");
        Kept shared = new Kept("shared");
        builder(new InMemoryTaskStore()).extension(shared).build();
        ExtensionContext first = shared.context;
        builder(new InMemoryTaskStore()).extension(shared).build();
        assertThat(shared.context).as("one instance, installed in two engines, gets a context each").isNotSameAs(first);
    }

    @Test
    void an_extension_pushes_in_its_own_transaction_and_reads_the_topics() {
        Kept kept = new Kept("kept");
        try (TaskEngine engine = builder(new InMemoryTaskStore()).extension(kept).build().prepare()) {
            ExtensionContext context = kept.context;
            context.transact("Pushing a run", transaction -> context.tasks(transaction).push(WORK, "kept", "x"));
            assertThatThrownBy(() -> context.transact("Pushing and failing", transaction -> {
                context.tasks(transaction).push(WORK, "undone", "x");
                throw new java.sql.SQLException("the extension's own write failed");
            })).isInstanceOf(io.github.khopland.pgtasks.TaskStorageException.class).hasMessageContaining("Pushing and failing");

            TaskTester tester = TaskTester.of(engine);
            assertThat(tester.counts(WORK).total()).as("the first push committed, the second rolled back").isEqualTo(1);
            assertThat(context.topics()).containsExactly(WORK);
            assertThat(context.admin().topic("work")).isPresent();
        }
    }

    @Test
    void a_woken_topic_asks_its_gates_again_at_once() throws Exception {
        AtomicBoolean open = new AtomicBoolean();
        AtomicInteger handled = new AtomicInteger();
        Kept kept = new Kept("valve");
        Extension valve = Extension.of("valve", context -> {
            kept.install(context);
            context.claimGate((topic, wanted) -> open.get() ? ClaimGate.Permit.ANY : ClaimGate.Permit.waitFor(Duration.ofHours(1)));
        });
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore()).extension(valve)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> handled.incrementAndGet(), options -> options.pollInterval(Duration.ofMinutes(5))))
            .build().start()) {
            engine.tasks().independently().push(WORK, "a", "x");
            EngineFixture.await("the gate to hold the topic", Duration.ofSeconds(5), () -> !engine.heldTopics().isEmpty());
            assertThat(engine.heldTopics()).singleElement().satisfies(held -> {
                assertThat(held.topic()).isEqualTo("work");
                assertThat(held.gate()).isEqualTo("valve");
                assertThat(held.until()).isAfter(held.since());
            });

            open.set(true);
            kept.context.wake(WORK);
            EngineFixture.await("the woken topic to claim", Duration.ofSeconds(5), () -> handled.get() == 1);
            EngineFixture.await("the hold to clear", Duration.ofSeconds(5), () -> engine.heldTopics().isEmpty());
        }
    }

    @Test
    void the_observers_are_told_when_a_gate_holds_a_topic_back() throws Exception {
        List<String> held = new CopyOnWriteArrayList<>();
        TaskObserver watching = new TaskObserver() {
            @Override
            public void held(TopicRef<?> topic, String gate, @org.jspecify.annotations.Nullable String reason, Duration delay) {
                held.add(topic.name() + " by " + gate + " (" + reason + ")");
            }
        };
        try (TaskEngine engine = builder(new InMemoryTaskStore()).observer(watching)
            .extension(Extension.of("pause", context -> context.claimGate((topic, wanted) -> ClaimGate.Permit.waitFor(Duration.ofSeconds(1), "paused"))))
            .build().start()) {
            EngineFixture.await("the observer to hear of it", Duration.ofSeconds(5), () -> !held.isEmpty());
            assertThat(held).first().isEqualTo("work by pause (paused)");
            assertThat(engine.health().held()).as("a hold with a reason shows at once, however short its wait").singleElement()
                .satisfies(topic -> {
                    assertThat(topic.gate()).isEqualTo("pause");
                    assertThat(topic.reason()).isEqualTo("paused");
                });
        }
    }

    @Test
    @DisplayName("A throttle doesn't show as a held topic, and a wait without a reason shows once it lasts a poll interval")
    void a_throttle_is_not_a_held_topic() throws Exception {
        AtomicInteger asked = new AtomicInteger();
        List<String> held = new CopyOnWriteArrayList<>();
        TaskObserver watching = new TaskObserver() {
            @Override
            public void held(TopicRef<?> topic, String gate, @org.jspecify.annotations.Nullable String reason, Duration delay) {
                held.add(gate);
            }
        };
        // Like a rate limit: every other claim waits a moment.
        ClaimGate throttle = (topic, wanted) -> asked.incrementAndGet() % 2 == 0 ? ClaimGate.Permit.waitFor(Duration.ofMillis(1)) : ClaimGate.Permit.ANY;
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore()).observer(watching)
            .extension(Extension.of("throttle", context -> context.claimGate(throttle)))
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { }, options -> options.pollInterval(Duration.ofMillis(50))))
            .build().start()) {
            EngineFixture.await("a few throttled claims", Duration.ofSeconds(5), () -> held.size() >= 3);
            assertThat(engine.heldTopics()).isEmpty();
        }

        ClaimGate stuck = (topic, wanted) -> ClaimGate.Permit.waitFor(Duration.ofMillis(10));
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .extension(Extension.of("stuck", context -> context.claimGate(stuck)))
            .configure("work", tasks -> tasks.eachTask(WORK, task -> { }, options -> options.pollInterval(Duration.ofMillis(200))))
            .build().start()) {
            EngineFixture.await("the hold to last a poll interval", Duration.ofSeconds(5), () -> !engine.heldTopics().isEmpty());
            assertThat(engine.heldTopics()).singleElement().satisfies(topic -> assertThat(topic.reason()).isNull());
        }
    }

    @Test
    @DisplayName("A gate learns which tasks it let through, and a listener reports each with its error and duration")
    void gates_learn_the_ids_and_listeners_report_errors() throws Exception {
        List<Long> claimed = new CopyOnWriteArrayList<>();
        List<BatchListener.Completed> seen = new CopyOnWriteArrayList<>();
        Extension watching = Extension.of("watching", context -> {
            context.claimGate(new ClaimGate() {
                @Override
                public Permit permit(TopicRef<?> topic, int wanted) {
                    return Permit.ANY;
                }

                @Override
                public void claimed(TopicRef<?> topic, List<Long> taskIds) {
                    claimed.addAll(taskIds);
                }
            });
            context.batchListener((topic, tasks) -> seen.addAll(tasks));
        });
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore()).extension(watching)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> {
                if (task.payload().equals("bad")) {
                    throw new IllegalStateException("the partner refused");
                }
            }))
            .build().start()) {
            long good = engine.tasks().independently().push(WORK, "good", "fine").id();
            long bad = engine.tasks().independently().push(WORK, "bad", "bad").id();
            EngineFixture.await("both results", Duration.ofSeconds(5), () -> seen.size() == 2);

            assertThat(claimed).containsExactlyInAnyOrder(good, bad);
            assertThat(seen).extracting(BatchListener.Completed::taskId).containsExactlyInAnyOrder(good, bad);
            assertThat(seen).filteredOn(task -> task.taskId() == bad).singleElement().satisfies(task -> {
                assertThat(task.failure()).isEqualTo(BatchListener.Failure.FAILED);
                assertThat(task.error()).isInstanceOf(IllegalStateException.class).hasMessage("the partner refused");
            });
            assertThat(seen).filteredOn(task -> task.taskId() == good).singleElement().satisfies(task -> {
                assertThat(task.succeeded()).isTrue();
                assertThat(task.error()).isNull();
                assertThat(task.duration().isNegative()).isFalse();
            });
        }
    }

    /** Thrown by the handler for a downstream system that refuses: a failure the policy doesn't count. */
    private static final class Refused extends RuntimeException {
        Refused() {
            super("downstream refused");
        }
    }

    @Test
    @DisplayName("A listener is told whose fault each failure is, so a breaker can leave bad input out")
    void a_listener_is_told_whose_fault_each_failure_is() throws Exception {
        ManualClock clock = ManualClock.startingNow();
        InMemoryTaskStore store = new InMemoryTaskStore(clock);
        BatchHandler<String> handler = batch -> batch.decide(task -> switch (task.payload()) {
            case "bad" -> throw new IllegalArgumentException("invalid row");
            case "down" -> Decision.failed("downstream failed");
            case "refused" -> throw new Refused();
            default -> Decision.success();
        });
        // One failure with a policy that allows more, then a policy that allows only one: the task
        // is out of attempts when it is next claimed, and fails without running.
        long spent;
        try (TaskEngine before = TaskEngine.builder().store(store)
            .configure("work", tasks -> tasks.topic(WORK, handler)).build().start()) {
            spent = before.tasks().independently().push(WORK, "spent", "down").id();
            TaskTester.of(before).awaitIdle();
        }
        List<BatchListener.Completed> seen = new CopyOnWriteArrayList<>();
        try (TaskEngine engine = TaskEngine.builder().store(store)
            .extension(Extension.of("listening", context -> context.batchListener((topic, tasks) -> seen.addAll(tasks))))
            .configure("work", tasks -> tasks.topic(WORK, handler,
                topic -> topic.retry(Retry.standard().attempts(1).deadLetterAt(IllegalArgumentException.class).notCounting(Refused.class))))
            .build().start()) {
            TaskTester tester = TaskTester.of(engine);
            for (String payload : List.of("ok", "bad", "down", "refused")) {
                engine.tasks().independently().push(WORK, payload, payload);
            }
            tester.awaitIdle();
            tester.advance(Duration.ofMinutes(1));
            tester.awaitIdle();
        }

        assertThat(seen).filteredOn(task -> task.taskId() != spent)
            .extracting(BatchListener.Completed::identifier, BatchListener.Completed::result, BatchListener.Completed::failure)
            .contains(
                tuple("ok", TaskObserver.Result.SUCCEEDED, BatchListener.Failure.NONE),
                tuple("bad", TaskObserver.Result.DEAD_LETTERED, BatchListener.Failure.TASK_FAULT),
                tuple("down", TaskObserver.Result.DEAD_LETTERED, BatchListener.Failure.FAILED),
                tuple("refused", TaskObserver.Result.UNAVAILABLE, BatchListener.Failure.FAILED));
        assertThat(seen).filteredOn(task -> task.taskId() == spent).singleElement()
            .satisfies(task -> assertThat(task.failure()).as("out of attempts before it ran").isEqualTo(BatchListener.Failure.TASK_FAULT));
        BatchListener.Completed made = new BatchListener.Completed(1, "a", 1, TaskObserver.Result.RETRYING);
        assertThat(made.failure()).isEqualTo(BatchListener.Failure.FAILED);
        IllegalArgumentException invalid = new IllegalArgumentException("invalid row");
        assertThat(made.withFailure(BatchListener.Failure.TASK_FAULT, invalid).withDuration(Duration.ofSeconds(2)))
            .satisfies(task -> {
                assertThat(task.failure()).isEqualTo(BatchListener.Failure.TASK_FAULT);
                assertThat(task.error()).isSameAs(invalid);
                assertThat(task.duration()).isEqualTo(Duration.ofSeconds(2));
                assertThat(task.taskId()).isEqualTo(1);
            });
    }

    @Test
    @DisplayName("A probe's counted failure doesn't use up an attempt")
    void a_probes_failures_dont_use_attempts() throws Exception {
        ManualClock clock = ManualClock.startingNow();
        AtomicInteger runs = new AtomicInteger();
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore(clock))
            .extension(Extension.of("probing", context -> context.claimGate((topic, wanted) -> ClaimGate.Permit.probe(1))))
            .configure("work", tasks -> tasks.topic(WORK, batch -> batch.decide(task -> {
                runs.incrementAndGet();
                return Decision.failed("downstream failed");
            }), topic -> topic.retry(Retry.standard().attempts(2))))
            .build().start()) {
            TaskTester tester = TaskTester.of(engine);
            long id = engine.tasks().independently().push(WORK, "a", "x").id();
            tester.awaitIdle();
            for (int failure = 1; failure < 5; failure++) {
                tester.advance(Duration.between(clock.instant(), tester.task(id).runAt()));
                tester.awaitIdle();
            }

            assertThat(runs).hasValue(5);
            assertThat(tester.task(id).state()).isEqualTo("retrying");
            assertThat(tester.task(id).attempt()).as("five failures, none counted").isZero();
            assertThat(tester.task(id).failures()).isEqualTo(5);
        }
        assertThatThrownBy(() -> ClaimGate.Permit.probe(0)).hasMessageContaining("A probe claims at least one task");
        assertThat(ClaimGate.Permit.atMost(3).probing()).isFalse();
        assertThat(ClaimGate.Permit.waitFor(Duration.ofSeconds(1), " paused ").reason()).isEqualTo("paused");
        assertThatThrownBy(() -> ClaimGate.Permit.waitFor(Duration.ofSeconds(1), " ")).hasMessageContaining("says something");
        assertThat(ClaimGate.Permit.waitFor(Duration.ofSeconds(1))).isEqualTo(new ClaimGate.Permit(0, Duration.ofSeconds(1)));
    }

    @Test
    void drain_jumps_the_clock_past_a_gates_wait() {
        ManualClock clock = ManualClock.startingNow();
        Instant opens = clock.instant().plus(Duration.ofMinutes(10));
        ClaimGate closedUntil = (topic, wanted) -> clock.instant().isBefore(opens)
            ? ClaimGate.Permit.waitFor(Duration.between(clock.instant(), opens)) : ClaimGate.Permit.ANY;
        try (TaskEngine engine = builder(new InMemoryTaskStore(clock)).extension(Extension.of("closed", context -> context.claimGate(closedUntil)))
            .build().start()) {
            TaskTester tester = TaskTester.of(engine);
            engine.tasks().independently().push(WORK, "a", "x");

            assertThat(tester.drain(WORK).total()).isZero();
            assertThat(clock.instant()).isAfterOrEqualTo(opens);
        }
    }

    @Test
    void advance_runs_the_extensions_every_node_jobs() throws Exception {
        ManualClock clock = ManualClock.startingNow();
        List<Instant> runs = new CopyOnWriteArrayList<>();
        Extension timed = Extension.of("timed", context -> context.job(HousekeepingJob.everyNode("timed", Duration.ofHours(1),
            () -> runs.add(context.clock().instant()))));
        try (TaskEngine engine = builder(new InMemoryTaskStore(clock)).extension(timed).build().start()) {
            EngineFixture.await("the first run", Duration.ofSeconds(5), () -> !runs.isEmpty());
            Instant later = TaskTester.of(engine).advance(Duration.ofMinutes(5));
            assertThat(runs).as("run again at the new time, not an hour later").contains(later);
        }
    }

    @Test
    @DisplayName("A channel's reload is told the messages' payloads, and nothing in particular when anything may have changed")
    void a_channel_reload_gets_the_payloads() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        List<Set<String>> reloads = new CopyOnWriteArrayList<>();
        Kept sender = new Kept("sender");
        try (TaskEngine listening = builder(store).extension(Extension.of("listening", context -> context.channel("pause", reloads::add))).build().start();
             TaskEngine sending = builder(store).extension(sender).build().start()) {
            EngineFixture.await("the heartbeat's reload", Duration.ofSeconds(5), () -> reloads.contains(Set.of()));
            sender.context.transact("Telling", transaction -> {
                sender.context.store().notify(transaction, "pause", "orders");
                sender.context.store().notify(transaction, "pause", "payments");
                return null;
            });
            assertThatThrownBy(() -> builder(store).extension(Extension.of("x", context -> context.channel("job", () -> { }))).build())
                .hasMessageContaining("other than ready, topic, job, data");
            EngineFixture.await("the messages", Duration.ofSeconds(5),
                () -> reloads.stream().flatMap(Set::stream).toList().containsAll(List.of("orders", "payments")));
            assertThat(listening.running()).isTrue();
            assertThat(sending.running()).isTrue();
        }
    }

    @Test
    @DisplayName("An extension's data is shared by every engine on the store, apart from other extensions', and rolls back with its transaction")
    void extension_data_is_shared_and_transactional() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        Kept first = new Kept("quota");
        Kept second = new Kept("quota");
        Kept other = new Kept("other");
        builder(store).extension(first, other).build();
        builder(store).extension(second).build();
        ExtensionData mine = first.context.data();

        first.context.transact("Counting", transaction -> mine.put(transaction, "tenant/north", "1"));
        Optional<ExtensionEntry> seen = second.context.transact("Reading", transaction -> second.context.data().get(transaction, "tenant/north"));
        assertThat(seen).as("another engine on the store, same extension").map(ExtensionEntry::value).contains("1");
        Optional<ExtensionEntry> elsewhere = other.context.transact("Reading", transaction -> other.context.data().get(transaction, "tenant/north"));
        assertThat(elsewhere).as("another extension's keys are its own").isEmpty();

        assertThatThrownBy(() -> first.context.transact("Counting and failing", transaction -> {
            mine.put(transaction, "tenant/north", "99");
            throw new IllegalStateException("the push it counted for failed");
        })).hasMessageContaining("the push it counted for failed");
        ExtensionEntry counted = first.context.transact("Counting", transaction -> mine.update(transaction, "tenant/north",
            count -> String.valueOf(Integer.parseInt(count == null ? "0" : count) + 1))).orElseThrow();
        assertThat(counted.value()).as("the failed write rolled back").isEqualTo("2");
        assertThat(counted.version()).isEqualTo(2);

        Optional<ExtensionEntry> stale = first.context.transact("Racing", transaction -> mine.putIf(transaction, "tenant/north", 1, "stale"));
        assertThat(stale).isEmpty();
        first.context.transact("Adding", transaction -> mine.put(transaction, "tenant/south", "5"));
        List<ExtensionEntry> tenants = first.context.transact("Listing", transaction -> mine.list(transaction, "tenant/"));
        assertThat(tenants).extracting(ExtensionEntry::key).containsExactly("tenant/north", "tenant/south");
        Optional<ExtensionEntry> removed = first.context.transact("Removing", transaction -> mine.update(transaction, "tenant/south", count -> null));
        assertThat(removed).isEmpty();
        List<ExtensionEntry> all = first.context.transact("Listing", transaction -> mine.list(transaction, ""));
        assertThat(all).hasSize(1);
        boolean notAtVersion = first.context.transact("Deleting", transaction -> mine.deleteIf(transaction, "tenant/north", 1));
        assertThat(notAtVersion).isFalse();
        boolean deleted = first.context.transact("Deleting", transaction -> mine.delete(transaction, "tenant/north"));
        assertThat(deleted).isTrue();

        assertThatThrownBy(() -> first.context.transact("Writing", transaction -> mine.put(transaction, "", "x")))
            .hasMessageContaining("A key is 1 to 200 characters");
        assertThatThrownBy(() -> first.context.transact("Writing", transaction -> mine.put(transaction, "big", "x".repeat(ExtensionEntry.MAX_VALUE + 1))))
            .hasMessageContaining("A value is at most");
    }

    @Test
    @DisplayName("A write to an extension's data reaches its reloads on every node, once it commits")
    void a_data_change_reaches_every_nodes_reloads() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        List<Set<String>> heard = new CopyOnWriteArrayList<>();
        Kept writer = new Kept("settings");
        // The same extension on both nodes, as in a deployment: each has the reload, the reading one records it.
        Extension listening = Extension.of("settings", context -> context.onDataChange(heard::add));
        Extension writing = Extension.of("settings", context -> {
            writer.install(context);
            context.onDataChange(keys -> { });
        });
        try (TaskEngine reading = builder(store).node("reading").extension(listening).build().start();
             TaskEngine writingEngine = builder(store).node("writing").extension(writing).build().start()) {
            EngineFixture.await("the heartbeat's reload", Duration.ofSeconds(5), () -> heard.contains(Set.of()));
            ExtensionData data = writer.context.data();
            assertThatThrownBy(() -> writer.context.transact("Changing and failing", transaction -> {
                data.put(transaction, "rolled-back", "x");
                throw new IllegalStateException("undone");
            })).hasMessageContaining("undone");
            writer.context.transact("Changing", transaction -> data.put(transaction, "limit", "10"));

            EngineFixture.await("the change", Duration.ofSeconds(5), () -> heard.stream().anyMatch(keys -> keys.contains("limit")));
            assertThat(heard).as("nothing from a write that rolled back").noneMatch(keys -> keys.contains("rolled-back"));
            assertThat(reading.running() && writingEngine.running()).isTrue();
        }
        assertThat(StoredData.parse(StoredData.payload("a:b", "c:d"))).isEqualTo(new StoredData.Change("a:b", "c:d"));
        assertThat(StoredData.parse(StoredData.payload("x".repeat(8_000), "key"))).as("too long to name the key")
            .isEqualTo(new StoredData.Change("x".repeat(8_000), ""));
        assertThat(StoredData.parse("nonsense")).isNull();
        assertThatThrownBy(() -> builder(store).extension(Extension.of("x", context -> context.channel("data", () -> { }))).build())
            .hasMessageContaining("other than ready, topic, job, data");
    }

    private static final TopicRef<String> NEXT = TopicRef.of("next", String.class);

    private static String value(Kept kept, String key) {
        Optional<ExtensionEntry> entry = kept.context.transact("Reading", transaction -> kept.context.data().get(transaction, key));
        return entry.map(ExtensionEntry::value).orElse("none");
    }

    @Test
    @DisplayName("A push hook runs in every push's transaction: it can refuse the push, and what it writes commits with it")
    void a_push_hook_refuses_and_writes_with_the_push() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        Kept quota = new Kept("quota");
        Extension quotas = Extension.of("quota", context -> {
            quota.install(context);
            context.pushHook(push -> {
                seen.add(push.topic().name() + " " + push.tasks().size() + " " + push.insertion());
                if (push.tasks().stream().anyMatch(task -> task.identifier().equals("refused"))) {
                    throw new IllegalArgumentException("over the quota");
                }
                context.data().update(push.transaction(), "pushes/" + push.topic().name(),
                    count -> String.valueOf((count == null ? 0 : Integer.parseInt(count)) + push.tasks().size()));
            });
        });
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore()).extension(quotas)
            .configure("work", tasks -> {
                tasks.eachTask(WORK, task -> task.push(NEXT, task.identifier(), "the next step"));
                tasks.eachTask(NEXT, task -> { });
            })
            .build().prepare()) {
            engine.tasks().independently().push(WORK, "a", "x");
            assertThatThrownBy(() -> engine.tasks().independently().push(WORK, "refused", "x"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("over the quota");
            TaskTester tester = TaskTester.of(engine);
            assertThat(tester.counts(WORK).total()).as("the refused push wrote nothing").isEqualTo(1);
            assertThat(value(quota, "pushes/work")).isEqualTo("1");

            engine.start();
            tester.awaitIdle();
            assertThat(value(quota, "pushes/next")).as("a handler's downstream push, in its completion's transaction").isEqualTo("1");
            assertThat(seen).contains("work 1 APPEND", "next 1 APPEND");
        }

        Extension unavailable = Extension.of("unavailable", context -> context.pushHook(push -> {
            throw new java.io.IOException("the quota service is down");
        }));
        try (TaskEngine engine = builder(new InMemoryTaskStore()).extension(unavailable).build().prepare()) {
            assertThatThrownBy(() -> engine.tasks().independently().push(WORK, "a", "x"))
                .isInstanceOf(io.github.khopland.pgtasks.TaskStorageException.class).hasMessageContaining("the quota service is down");
        }
    }

    @Test
    @DisplayName("A completion hook writes with the completion and can push the next step; one that throws fails the completion")
    void a_completion_hook_commits_with_the_completion() throws Exception {
        AtomicBoolean refuseOnce = new AtomicBoolean(true);
        List<String> heard = new CopyOnWriteArrayList<>();
        Kept audit = new Kept("audit");
        Extension auditing = Extension.of("audit", context -> {
            audit.install(context);
            context.completionHook(batch -> {
                for (BatchListener.Completed task : batch.tasks()) {
                    context.data().put(batch.transaction(), "done/" + batch.topic().name() + "/" + task.identifier(),
                        task.result() + " on attempt " + task.attempt());
                    if (batch.topic().equals(WORK) && task.succeeded()) {
                        context.tasks(batch.transaction()).push(NEXT, task.identifier(), "the next step");
                    }
                }
                if (batch.topic().equals(WORK) && refuseOnce.getAndSet(false)) {
                    throw new IllegalStateException("the audit is full");
                }
            });
            context.batchListener((topic, tasks) -> tasks.forEach(task -> heard.add(topic.name() + " " + task.identifier() + " " + task.failure())));
        });
        ManualClock clock = ManualClock.startingNow();
        try (TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore(clock)).extension(auditing)
            .configure("work", tasks -> {
                tasks.eachTask(WORK, task -> { });
                tasks.eachTask(NEXT, task -> { });
            })
            .build().start()) {
            TaskTester tester = TaskTester.of(engine);
            long id = engine.tasks().independently().push(WORK, "a", "x").id();
            tester.drain(WORK);
            tester.drain(NEXT);

            assertThat(value(audit, "done/work/a")).as("the failed completion's write rolled back with it").isEqualTo("SUCCEEDED on attempt 2");
            assertThat(tester.task(id).state()).isEqualTo("succeeded");
            assertThat(value(audit, "done/next/a")).as("the next step, pushed with the completion, ran").isEqualTo("SUCCEEDED on attempt 1");
            assertThat(heard).containsSubsequence("work a FAILED", "work a NONE", "next a NONE");
            assertThat(heard).as("one next step: the failed completion's push rolled back").filteredOn(each -> each.startsWith("next")).hasSize(1);
        }
    }

    @Test
    void operator_views_are_listed_and_each_name_is_taken_once() {
        OperatorView schedules = new OperatorView() {
            @Override
            public String name() {
                return "schedules";
            }

            @Override
            public String title() {
                return "Recurring tasks";
            }

            @Override
            public List<Column> columns() {
                return List.of(Column.of("topic", "Topic", Column.Kind.TOPIC), Column.text("name", "Name"));
            }

            @Override
            public List<Row> rows() {
                return List.of(Row.of("work/nightly", Map.of("topic", "work", "name", "nightly"), "run"));
            }

            @Override
            public List<Action> actions() {
                return List.of(Action.of("run", "Run now", null, key -> { }));
            }
        };
        TaskEngine engine = builder(new InMemoryTaskStore()).extension(Extension.of("recurring", context -> context.operatorView(schedules))).build();
        assertThat(engine.operatorViews()).containsExactly(schedules);
        assertThat(schedules.topic("work")).isNull();

        assertThatThrownBy(() -> builder(new InMemoryTaskStore()).extension(
            Extension.of("a", context -> context.operatorView(schedules)), Extension.of("b", context -> context.operatorView(schedules))).build())
            .hasMessageContaining("Another operator view is named 'schedules'");
        assertThatThrownBy(() -> OperatorView.Action.of("Run Now", "Run now", null, key -> { })).hasMessageContaining("Invalid action name");
        assertThat(OperatorView.TopicState.of("paused", "resume").actions()).containsExactly("resume");

        OperatorView paused = new OperatorView() {
            @Override
            public String name() {
                return "pause";
            }

            @Override
            public String title() {
                return "Paused topics";
            }

            @Override
            public TopicState topic(String topic) {
                return topic.equals("work") ? TopicState.of("paused", "resume") : null;
            }
        };
        assertThat(paused.topics(Set.of("work", "other"))).as("each topic by default, leaving out those it says nothing about")
            .containsOnlyKeys("work");
    }
}
