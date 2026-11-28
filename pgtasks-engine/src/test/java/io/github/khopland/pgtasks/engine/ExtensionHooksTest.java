package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.BatchHandler;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.HousekeepingJob;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.engine.extension.SettingKey;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
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
        AtomicBoolean stopped = new AtomicBoolean();
        Extension failing = Extension.of("failing", context -> {
            context.onStart(() -> {
                throw new IllegalStateException("its table is missing");
            });
            context.onStop(() -> stopped.set(true));
        });
        TaskEngine engine = TaskEngine.builder().store(store).extension(failing)
            .configure("work", tasks -> tasks.eachTask(WORK, task -> handled.incrementAndGet()))
            .build();
        assertThatThrownBy(engine::start).hasMessageContaining("its table is missing");
        assertThat(engine.running()).isFalse();
        assertThat(handled).as("nothing was claimed").hasValue(0);
        assertThat(stopped).as("the stop actions ran").isTrue();
        assertThat(engine.admin().nodes()).as("the node that failed to start is gone").isEmpty();
    }

    @Test
    @DisplayName("A leader job runs under a lease of its own, on a node that has it, whoever runs the housekeeping")
    void a_leader_job_has_a_lease_of_its_own() throws Exception {
        InMemoryTaskStore store = new InMemoryTaskStore();
        AtomicInteger runs = new AtomicInteger();
        Kept kept = new Kept("cleanup");
        Extension cleanup = Extension.of("cleanup", context -> {
            kept.install(context);
            context.job(HousekeepingJob.leader("cleanup", Duration.ofMillis(10), runs::incrementAndGet));
        });
        try (TaskEngine plain = builder(store).node("plain").build().start()) {
            EngineFixture.await("the plain node to lead housekeeping", Duration.ofSeconds(5), plain::leadsHousekeeping);
            try (TaskEngine extended = builder(store).node("extended").extension(cleanup).build().start()) {
                EngineFixture.await("the leader job to run", Duration.ofSeconds(5), () -> runs.get() > 0);

                assertThat(plain.leadsHousekeeping()).as("the housekeeping lease stays where it was").isTrue();
                assertThat(kept.context.leads("cleanup")).isTrue();
                assertThat(kept.context.leads("other")).isFalse();
                assertThat(plain.admin().nodes()).filteredOn(node -> node.name().equals("extended")).singleElement()
                    .satisfies(node -> assertThat(node.leases()).as("its job's lease, not the housekeeping lease").containsExactly("cleanup"));
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
            .build()).hasMessageContaining("'promotion' of its own");
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
        Kept kept = new Kept("slow");
        Extension slow = Extension.of("slow", context -> {
            kept.install(context);
            context.job(HousekeepingJob.everyNode("slow", Duration.ofHours(1), runs::incrementAndGet));
        });
        try (TaskEngine engine = builder(new InMemoryTaskStore()).extension(slow).build().start()) {
            EngineFixture.await("the first run", Duration.ofSeconds(5), () -> runs.get() == 1);
            kept.context.runSoon("slow");
            EngineFixture.await("the run asked for", Duration.ofSeconds(5), () -> runs.get() == 2);
            assertThatThrownBy(() -> kept.context.runSoon("other")).hasMessageContaining("no extension job named 'other'");
            assertThat(engine.running()).isTrue();
        }
        kept.context.runSoon("slow");
        assertThat(runs).as("nothing once stopped").hasValue(2);
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
            public void held(TopicRef<?> topic, String gate, Duration delay) {
                held.add(topic.name() + " by " + gate);
            }
        };
        try (TaskEngine engine = builder(new InMemoryTaskStore()).observer(watching)
            .extension(Extension.of("pause", context -> context.claimGate((topic, wanted) -> ClaimGate.Permit.waitFor(Duration.ofMinutes(1)))))
            .build().start()) {
            EngineFixture.await("the observer to hear of it", Duration.ofSeconds(5), () -> !held.isEmpty());
            assertThat(held).first().isEqualTo("work by pause");
            assertThat(engine.health().held()).extracting(EngineHealth.HeldTopic::gate).containsExactly("pause");
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
        assertThat(new BatchListener.Completed(1, "a", 1, TaskObserver.Result.RETRYING).failure()).isEqualTo(BatchListener.Failure.FAILED);
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
        assertThatThrownBy(() -> new ClaimGate.Permit(0, Duration.ofSeconds(1), true)).hasMessageContaining("probe");
        assertThat(ClaimGate.Permit.atMost(3).probing()).isFalse();
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
            EngineFixture.await("the messages", Duration.ofSeconds(5),
                () -> reloads.stream().flatMap(Set::stream).toList().containsAll(List.of("orders", "payments")));
            assertThat(listening.running()).isTrue();
            assertThat(sending.running()).isTrue();
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
    }
}
