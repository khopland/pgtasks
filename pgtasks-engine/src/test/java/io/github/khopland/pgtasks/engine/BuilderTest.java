package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Period;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.EffectiveSettings.Source;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The configuration model: defaults, configurers, globals, overrides and validation. */
class BuilderTest {

    record Notice(String text) {
    }

    private static final TopicRef<Notice> NOTIFY = TopicRef.of("notify", Notice.class);
    private static final PayloadCodec<Notice> NOTICES = PayloadCodec.of(Notice::text, Notice::new);

    private TaskEngine engine;

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.stop(Duration.ofSeconds(5));
        }
    }

    private static TaskConfigurer notifyTasks() {
        return tasks -> tasks.codec(Notice.class, NOTICES).topic(NOTIFY, batch -> batch.decide(task -> Decision.success()));
    }

    private static TaskConfigurer assessmentTasks() {
        return tasks -> tasks
            .defaults(defaults -> defaults.concurrency(8))
            .topic("assessment", String.class, batch -> batch.decide(task -> Decision.success()), topic -> topic.ordering(Ordering.SINGULAR_BY_IDENTIFIER))
            .topics(List.of("reassessment", "appeal"), String.class, batch -> batch.decide(task -> Decision.success()));
    }

    private static TaskGlobals globals() {
        return globals -> globals
            .housekeeping(housekeeping -> housekeeping.retain(Outcome.SUCCEEDED, Period.ofDays(90)));
    }

    private static EffectiveSettings settings(TaskEngine engine, String topic) {
        return engine.effectiveSettings().stream().filter(each -> each.topic().equals(topic)).findFirst().orElseThrow();
    }

    /** Review 2026-11-21: resources in the global defaults were accepted and never used. */
    @Test
    void resources_from_every_layer_of_defaults_are_opened_in_order() throws Exception {
        List<String> opened = new CopyOnWriteArrayList<>();
        Function<String, BatchResource<String>> resource = name -> (topic, tasks) -> {
            opened.add(name + ":" + topic.name());
            return name;
        };
        engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .globals(globals -> globals.defaults(defaults -> defaults.resources(resource.apply("global"))))
            .configure("notify", tasks -> tasks
                .defaults(defaults -> defaults.resources(resource.apply("module")))
                .codec(Notice.class, NOTICES)
                .topic(NOTIFY, batch -> batch.decide(task -> Decision.success()), topic -> topic.resources(resource.apply("topic"))))
            .build().start();
        engine.tasks().independently().push(NOTIFY, "anna", new Notice("hello"));
        TaskTester.of(engine).awaitIdle();

        assertThat(opened).containsExactly("global:notify", "module:notify", "topic:notify");
    }

    @Test
    void a_topic_needs_only_a_name_a_type_and_a_handler() throws Exception {
        engine = TaskEngine.builder().store(new InMemoryTaskStore()).configure(notifyTasks()).build().start();

        EffectiveSettings notify = settings(engine, "notify");
        assertThat(notify.get("batch-size")).isEqualTo(new EffectiveSettings.Setting("100", Source.DEFAULT));
        assertThat(notify.get("concurrency").value()).isEqualTo("2");
        assertThat(notify.get("ordering").value()).isEqualTo("FIFO");
        assertThat(notify.get("retry").value()).isEqualTo("for 7 days after the first failure (about 343 attempts), from PT10S x2.0 up to PT30M");
        assertThat(notify.get("retain.succeeded").value()).isEqualTo("P30D");
        assertThat(notify.get("retain.failed").value()).isEqualTo("forever");

        TaskTester tester = TaskTester.of(engine);
        engine.tasks().independently().push(NOTIFY, "x", new Notice("hello"));
        tester.awaitIdle();
        assertThat(tester.pushed(NOTIFY)).singleElement().extracting(creation -> creation.payload().text()).isEqualTo("hello");
    }

    @Test
    void layers_override_each_other_and_report_where_each_value_came_from() {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .globals(globals())
            .configure("assessment-module", assessmentTasks())
            .configure("notify-module", notifyTasks())
            .overrides(Map.of(
                "topics.assessment.concurrency", "16",
                "topics.assessment.retry.max-attempts", "2",
                "topics.appeal.poll-interval", "5s",
                "topics.appeal.retain.filtered", "forever"))
            .build();

        EffectiveSettings assessment = settings(engine, "assessment");
        assertThat(assessment.module()).isEqualTo("assessment-module");
        assertThat(assessment.get("concurrency")).isEqualTo(new EffectiveSettings.Setting("16", Source.OVERRIDE));
        assertThat(assessment.get("retry").value()).startsWith("2 attempts");
        assertThat(assessment.get("retry").source()).isEqualTo(Source.OVERRIDE);
        assertThat(engine.topics()).filteredOn(topic -> topic.ref().name().equals("assessment"))
            .singleElement().satisfies(topic -> assertThat(topic.settings().retry().maxAttempts()).isEqualTo(2));
        assertThat(assessment.get("ordering")).isEqualTo(new EffectiveSettings.Setting("SINGULAR_BY_IDENTIFIER", Source.TOPIC));
        assertThat(assessment.get("retain.succeeded")).isEqualTo(new EffectiveSettings.Setting("P90D", Source.GLOBAL_DEFAULTS));

        EffectiveSettings appeal = settings(engine, "appeal");
        assertThat(appeal.get("concurrency")).isEqualTo(new EffectiveSettings.Setting("8", Source.MODULE_DEFAULTS));
        assertThat(appeal.get("poll-interval")).isEqualTo(new EffectiveSettings.Setting("PT5S", Source.OVERRIDE));
        assertThat(appeal.get("retain.filtered")).isEqualTo(new EffectiveSettings.Setting("forever", Source.OVERRIDE));

        EffectiveSettings notify = settings(engine, "notify");
        assertThat(notify.get("concurrency").source()).as("another module's defaults don't leak").isEqualTo(Source.DEFAULT);
    }

    @Test
    void every_problem_is_reported_at_once() {
        assertThatThrownBy(() -> TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("first", tasks -> tasks.topic("dup", String.class, batch -> batch.decide(task -> Decision.success())))
            .configure("second", tasks -> tasks
                .topic("dup", String.class, batch -> batch.decide(task -> Decision.success()))
                .topic(NOTIFY, batch -> batch.decide(task -> Decision.success())))
            .overrides(Map.of("topics.missing.concurrency", "2", "topics.dup.concurency", "2", "limits.nope.rate", "1/s"))
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Topic dup is registered by both first and second")
            .hasMessageContaining("no payload codec for " + Notice.class.getName())
            .hasMessageContaining("topics.missing.concurrency names an unknown topic")
            .hasMessageContaining("topics.dup.concurency: unknown setting")
            .hasMessageContaining("Unknown override limits.nope.rate");
    }

    @Test
    void retry_overrides_that_contradict_each_other_fail_the_build() {
        assertThatThrownBy(() -> TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("first", tasks -> tasks
                .topic("one", String.class, batch -> batch.decide(task -> Decision.success()))
                .topic("two", String.class, batch -> batch.decide(task -> Decision.success())))
            .overrides(Map.of(
                "topics.one.retry.first-backoff", "10m",
                "topics.one.retry.max-backoff", "1m",
                "topics.two.retry.max-attempts", "3",
                "topics.two.retry.give-up-after", "1d"))
            .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("topics.one: retry.first-backoff (10m) is above retry.max-backoff (1m)")
            .hasMessageContaining("topics.two: retry.max-attempts and retry.give-up-after both say when to give up");
    }

    @Test
    void a_backoff_override_alone_moves_the_other_end_of_the_ramp_with_it() {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .configure("first", tasks -> tasks
                .topic("one", String.class, batch -> batch.decide(task -> Decision.success()))
                .topic("two", String.class, batch -> batch.decide(task -> Decision.success())))
            .overrides(Map.of(
                "topics.one.retry.first-backoff", "1h",
                "topics.two.retry.max-backoff", "1s"))
            .build();

        Map<String, Retry> retries = new HashMap<>();
        engine.topics().forEach(topic -> retries.put(topic.ref().name(), topic.settings().retry()));
        assertThat(retries.get("one").initial()).isEqualTo(Duration.ofHours(1));
        assertThat(retries.get("one").max()).isEqualTo(Duration.ofHours(1));
        assertThat(retries.get("two").initial()).isEqualTo(Duration.ofSeconds(1));
        assertThat(retries.get("two").max()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void the_in_memory_store_is_never_chosen_automatically() {
        assertThatThrownBy(() -> TaskEngine.builder().configure(notifyTasks()).build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No task store")
            .hasMessageContaining("never chosen automatically");
    }

    @Test
    void retention_reaches_the_running_topics() throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(ManualClock.startingNow()))
            .globals(globals())
            .configure(assessmentTasks())
            .build();

        Topic<?> assessment = engine.topics().stream().filter(topic -> topic.ref().name().equals("assessment")).findFirst().orElseThrow();
        assertThat(assessment.settings().retention()).containsEntry(Outcome.SUCCEEDED, Period.ofDays(90)).doesNotContainKey(Outcome.FAILED);
    }

    @Test
    void durations_parse_the_way_people_write_them() {
        assertThat(TaskEngineBuilder.duration("500ms")).isEqualTo(Duration.ofMillis(500));
        assertThat(TaskEngineBuilder.duration("PT2M")).isEqualTo(Duration.ofMinutes(2));
        assertThat(TaskEngineBuilder.period("30d")).isEqualTo(Period.ofDays(30));
    }

    @Test
    void a_list_of_topic_refs_registers_each_with_the_same_handler_and_options() throws Exception {
        List<TopicRef<String>> years = List.of(TopicRef.of("assessment-2025", String.class), TopicRef.of("assessment-2026", String.class));
        TaskEngine engine = TaskEngine.builder().store(new InMemoryTaskStore())
            .configure("assessment-module", tasks -> tasks.topics(years, batch -> batch.decide(task -> Decision.success()),
                options -> options.concurrency(4)))
            .build();

        assertThat(engine.topics()).extracting(topic -> topic.ref().name()).containsExactly("assessment-2025", "assessment-2026");
        assertThat(engine.topics()).extracting(topic -> topic.settings().concurrency()).containsOnly(4);
    }
}
