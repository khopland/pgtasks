package org.example.task2.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.example.task2.admin.TaskView;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.example.task2.Decision;
import org.example.task2.Tasks;
import org.example.task2.TopicRef;
import org.example.task2.engine.EffectiveSettings;
import org.example.task2.engine.EngineFixture;
import org.example.task2.engine.Rate;
import org.example.task2.engine.TaskConfigurer;
import org.example.task2.engine.TaskEngine;
import org.example.task2.engine.TaskGlobals;
import org.example.task2.test.PostgresDatabase;
import org.example.task2.test.TestPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

class TaskAutoConfigurationTest {

    static final TopicRef<String> NOTIFY = TopicRef.of("notify", String.class);
    static final TopicRef<String> ASSESSMENT = TopicRef.of("assessment", String.class);

    private PostgresDatabase database;

    @BeforeEach
    void setUp() {
        database = TestPostgres.createDatabase();
    }

    @AfterEach
    void tearDown() {
        database.close();
    }

    /** Postgres through Spring Boot's own DataSource and transaction manager. */
    private ApplicationContextRunner postgres() {
        return new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class,
                TaskAutoConfiguration.class))
            .withPropertyValues(
                "spring.datasource.url=" + database.jdbcUrl(),
                "spring.datasource.username=" + database.user(),
                "spring.datasource.password=" + database.password());
    }

    private static ApplicationContextRunner memory() {
        return new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TaskAutoConfiguration.class))
            .withPropertyValues("task.store=memory");
    }

    /** Two modules, each with its own configurer and handler. */
    @Configuration(proxyBeanMethods = false)
    static class Modules {
        final List<String> handled = new CopyOnWriteArrayList<>();

        @Bean
        TaskConfigurer notifyTasks() {
            return tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> {
                handled.add(task.payload());
                return Decision.success();
            }));
        }

        @Bean
        TaskConfigurer assessmentTasks() {
            return tasks -> tasks.defaults(options -> options.concurrency(8))
                .topic(ASSESSMENT, batch -> batch.decide(task -> Decision.success()), options -> options.limit("register"));
        }

        @Bean
        TaskGlobals taskSettings() {
            return globals -> globals.limit("register", limit -> limit.rate(Rate.perSecond(200)));
        }
    }

    @Test
    void configurer_beans_are_merged_into_one_engine_that_runs_with_the_context() {
        postgres().withUserConfiguration(Modules.class).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(TaskEngine.class).hasSingleBean(Tasks.class);
            TaskEngine engine = context.getBean(TaskEngine.class);
            assertThat(engine.processing()).containsExactlyInAnyOrder("notify", "assessment");
            assertThat(engine.effectiveSettings()).extracting(EffectiveSettings::topic, EffectiveSettings::module)
                .containsExactlyInAnyOrder(org.assertj.core.groups.Tuple.tuple("notify", "notifyTasks"),
                    org.assertj.core.groups.Tuple.tuple("assessment", "assessmentTasks"));
            assertThat(setting(engine, "assessment", "concurrency")).isEqualTo(new EffectiveSettings.Setting("8", EffectiveSettings.Source.MODULE_DEFAULTS));

            context.getBean(Tasks.class).independently().push(NOTIFY, "anna", "hello");
            EngineFixture.await("the task ran", Duration.ofSeconds(5), () -> context.getBean(Modules.class).handled.contains("hello"));
        });
    }

    @Test
    void task_properties_override_what_the_code_sets() {
        postgres().withUserConfiguration(Modules.class)
            .withPropertyValues("task.topics.assessment.concurrency=3", "task.topics.assessment.retry.max-attempts=2",
                "task.topics.notify.poll-interval=5s", "task.limits.register.rate=40/s", "task.node=pod-1")
            .run(context -> {
                assertThat(context).hasNotFailed();
                TaskEngine engine = context.getBean(TaskEngine.class);
                assertThat(setting(engine, "assessment", "concurrency")).isEqualTo(new EffectiveSettings.Setting("3", EffectiveSettings.Source.OVERRIDE));
                assertThat(setting(engine, "assessment", "retry").value()).startsWith("2 attempts");
                assertThat(setting(engine, "notify", "poll-interval")).isEqualTo(new EffectiveSettings.Setting("PT5S", EffectiveSettings.Source.OVERRIDE));
                assertThat(engine.limits().find("register")).hasValueSatisfying(limit -> assertThat(limit.configured().perSecond()).isEqualTo(40.0));
                assertThat(database.rows("SELECT name FROM tasks.node")).containsExactly(List.of("pod-1"));
            });
    }

    @Test
    void an_override_for_a_topic_no_configurer_defines_fails_startup() {
        postgres().withUserConfiguration(Modules.class).withPropertyValues("task.topics.asessment.concurrency=3").run(context ->
            assertThat(context).hasFailed().getFailure().rootCause().hasMessageContaining("topics.asessment.concurrency names an unknown topic"));
    }

    @Test
    void two_configurers_registering_one_topic_fail_startup_naming_both_beans() {
        postgres().withUserConfiguration(Modules.class)
            .withBean("lateNotifyTasks", TaskConfigurer.class, () -> tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> Decision.success())))
            .run(context -> assertThat(context).hasFailed().getFailure().rootCause()
                .hasMessageContainingAll("Topic notify is registered by both", "notifyTasks", "lateNotifyTasks"));
    }

    @Test
    void without_a_data_source_startup_fails_instead_of_choosing_memory() {
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(TaskAutoConfiguration.class)).run(context ->
            assertThat(context).hasFailed().getFailure().rootCause().hasMessage(TaskAutoConfiguration.NO_DATA_SOURCE));
    }

    @Test
    void a_push_joins_the_spring_transaction_on_postgres() {
        postgres().withUserConfiguration(Modules.class).withPropertyValues("task.topics.notify.poll-interval=1h").run(context -> {
            Tasks tasks = context.getBean(Tasks.class);
            TransactionTemplate transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            context.getBean(TaskEngine.class).admin().pause("notify");

            assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
                tasks.push(NOTIFY, "anna", "rolled back");
                assertThat(database.count("SELECT count(*) FROM tasks.task")).as("not visible before the commit").isZero();
                throw new IllegalStateException("the application's work failed");
            })).hasMessage("the application's work failed");
            assertThat(database.count("SELECT count(*) FROM tasks.task")).isZero();

            transaction.executeWithoutResult(status -> tasks.push(NOTIFY, "anna", "committed"));
            assertThat(database.rows("SELECT payload FROM tasks.task")).containsExactly(List.of("committed"));

            assertThatThrownBy(() -> tasks.push(NOTIFY, "anna", "no transaction")).hasMessageContaining("there is none");
        });
    }

    @Test
    void a_configurer_registered_as_a_plain_singleton_is_named_by_its_bean_name() {
        memory().withInitializer(context -> context.getBeanFactory().registerSingleton("manualTasks",
                (TaskConfigurer) tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> Decision.success()))))
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBean(TaskEngine.class).effectiveSettings()).extracting(EffectiveSettings::module).containsExactly("manualTasks");
            });
    }

    @Test
    void with_task_enabled_false_the_node_only_pushes_and_others_process() {
        postgres().withUserConfiguration(Modules.class).withPropertyValues("task.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            TaskEngine engine = context.getBean(TaskEngine.class);
            assertThat(engine.running()).isFalse();
            TransactionTemplate transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            transaction.executeWithoutResult(status -> context.getBean(Tasks.class).push(NOTIFY, "anna", "pushed by the producer"));

            Thread.sleep(300);
            assertThat(database.count("SELECT count(*) FROM tasks.queue WHERE state = 'ready'")).as("nobody processes here").isEqualTo(1);
            assertThat(database.count("SELECT count(*) FROM tasks.node")).as("no node row for a producer").isZero();
            assertThat(new TaskHealthIndicator(engine, false).health().getStatus()).isEqualTo(org.springframework.boot.health.contributor.Status.UP);

            List<String> handled = new CopyOnWriteArrayList<>();
            try (TaskEngine consumer = TaskEngine.builder().dataSource(database.dataSource())
                .configure("notify", tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> {
                    handled.add(task.payload());
                    return Decision.success();
                })))
                .build().start()) {
                EngineFixture.await("a processing node to run it", Duration.ofSeconds(5), () -> handled.contains("pushed by the producer"));
            }
        });
    }

    /** A service that pushes, used by a handler: the usual shape of a module. */
    static class OrderService {
        final Tasks tasks;
        final List<String> handled = new CopyOnWriteArrayList<>();

        OrderService(Tasks tasks) {
            this.tasks = tasks;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class HandlerUsingAPushingService {
        @Bean
        OrderService orderService(Tasks tasks) {
            return new OrderService(tasks);
        }

        @Bean
        TaskConfigurer notifyTasks(OrderService service) {
            return tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> {
                service.handled.add(task.payload());
                return Decision.success();
            }));
        }
    }

    /** Review 2026-11-21: getBeansOfType left the configurer out while it was in creation, and the engine started without its topic. */
    @Test
    void a_configurer_whose_handler_uses_a_service_that_pushes_is_not_left_out() {
        memory().withUserConfiguration(HandlerUsingAPushingService.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(TaskEngine.class).topics()).extracting(topic -> topic.ref().name()).containsExactly("notify");
            context.getBean(OrderService.class).tasks.independently().push(NOTIFY, "anna", "pushed by the service");
            EngineFixture.await("the task ran", Duration.ofSeconds(5),
                () -> context.getBean(OrderService.class).handled.contains("pushed by the service"));
        });
    }

    static class EngineWatcher {
        EngineWatcher(TaskEngine engine) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class HandlerUsingTheEngine {
        @Bean
        EngineWatcher engineWatcher(TaskEngine engine) {
            return new EngineWatcher(engine);
        }

        @Bean
        TaskConfigurer notifyTasks(EngineWatcher watcher) {
            return tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> Decision.success()));
        }
    }

    @Test
    void a_configurer_that_needs_the_engine_itself_fails_startup_saying_why() {
        memory().withUserConfiguration(HandlerUsingTheEngine.class).run(context ->
            assertThat(context).hasFailed().getFailure().hasStackTraceContaining("The TaskConfigurer bean 'notifyTasks' depends on the task engine"));
    }

    record Order(String id, int quantity) {
    }

    static final TopicRef<Order> ORDERS = TopicRef.of("orders", Order.class);

    @Configuration(proxyBeanMethods = false)
    static class RecordTopic {
        final List<Order> handled = new CopyOnWriteArrayList<>();

        @Bean
        TaskConfigurer orderTasks() {
            return tasks -> tasks.topic(ORDERS, batch -> batch.decide(task -> {
                handled.add(task.payload());
                return Decision.success();
            }));
        }
    }

    @Test
    void a_record_payload_needs_no_codec_it_is_stored_as_json() {
        memory().withUserConfiguration(RecordTopic.class).run(context -> {
            assertThat(context).hasNotFailed();
            TaskEngine engine = context.getBean(TaskEngine.class);
            long id = context.getBean(Tasks.class).independently().push(ORDERS, "o-1", new Order("o-1", 3)).id();
            EngineFixture.await("the order", Duration.ofSeconds(5), () -> !context.getBean(RecordTopic.class).handled.isEmpty());

            assertThat(context.getBean(RecordTopic.class).handled).containsExactly(new Order("o-1", 3));
            TaskView view = engine.admin().find(id).orElseThrow();
            assertThat(view.payloadType()).isEqualTo("json");
            assertThat(view.payload()).isEqualTo("{\"id\":\"o-1\",\"quantity\":3}");
        });
    }

    record Greeted(String name) {
    }

    @Configuration(proxyBeanMethods = false)
    static class GreetingRoute {
        @Bean
        TaskConfigurer greetingTasks() {
            return tasks -> tasks
                .onEvent(Greeted.class, greeted -> greeted.name().isEmpty() ? null
                    : new org.example.task2.Push<>(NOTIFY, org.example.task2.Insertion.APPEND, org.example.task2.Creation.of(greeted.name(), "hello " + greeted.name())))
                .topic(NOTIFY, batch -> batch.decide(task -> Decision.success()));
        }
    }

    @Test
    void an_event_becomes_a_task_in_the_publishers_transaction() {
        postgres().withUserConfiguration(GreetingRoute.class).withPropertyValues("task.topics.notify.poll-interval=1h").run(context -> {
            TransactionTemplate transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            context.getBean(TaskEngine.class).admin().pause("notify");

            assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
                context.publishEvent(new Greeted("anna"));
                throw new IllegalStateException("the publisher's work failed");
            })).hasMessage("the publisher's work failed");
            assertThat(database.count("SELECT count(*) FROM tasks.task")).as("rolled back with the publisher").isZero();

            transaction.executeWithoutResult(status -> context.publishEvent(new Greeted("bob")));
            context.publishEvent(new Greeted("carl"));
            context.publishEvent(new Greeted(""));
            assertThat(database.rows("SELECT payload FROM tasks.task ORDER BY id")).as("committed with it, or on its own without one")
                .containsExactly(List.of("hello bob"), List.of("hello carl"));
        });
    }

    @Test
    void stopping_the_context_drains_the_engine() {
        postgres().withUserConfiguration(Modules.class).run(context -> {
            TaskEngine engine = context.getBean(TaskEngine.class);
            context.close();
            assertThat(engine.processing()).isEmpty();
            assertThat(database.count("SELECT count(*) FROM tasks.lease")).isZero();
        });
    }

    @Test
    void a_context_stopped_and_started_again_processes_again() {
        memory().withUserConfiguration(Modules.class).run(context -> {
            context.stop();
            assertThat(context.getBean(TaskEngine.class).running()).isFalse();
            context.start();
            assertThat(context.getBean(TaskEngine.class).running()).isTrue();

            context.getBean(Tasks.class).independently().push(NOTIFY, "anna", "after the restart");
            EngineFixture.await("the task ran", Duration.ofSeconds(5), () -> context.getBean(Modules.class).handled.contains("after the restart"));
        });
    }

    @Test
    void an_engine_of_the_applications_own_wins() {
        memory().withUserConfiguration(Modules.class)
            .withBean(TaskEngine.class, () -> TaskEngine.builder().store(new org.example.task2.memory.InMemoryTaskStore()).build())
            .run(context -> assertThat(context.getBean(TaskEngine.class).processing()).isEmpty());
    }

    /** In memory, a push inside a Spring transaction is held back until it commits. */
    @Test
    void in_memory_a_push_waits_for_the_commit() {
        memory().withUserConfiguration(Modules.class).withBean(PlatformTransactionManager.class, NoResources::new).run(context -> {
            assertThat(context).hasNotFailed();
            Tasks tasks = context.getBean(Tasks.class);
            Modules modules = context.getBean(Modules.class);
            TransactionTemplate transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));

            assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
                tasks.push(NOTIFY, "anna", "rolled back");
                throw new IllegalStateException("the application's work failed");
            })).hasMessage("the application's work failed");
            transaction.executeWithoutResult(status -> tasks.push(NOTIFY, "anna", "committed"));

            EngineFixture.await("the committed task ran", Duration.ofSeconds(5), () -> modules.handled.contains("committed"));
            assertThat(modules.handled).containsExactly("committed");
        });
    }

    private static EffectiveSettings.Setting setting(TaskEngine engine, String topic, String key) {
        return engine.effectiveSettings().stream().filter(each -> each.topic().equals(topic)).findFirst().orElseThrow().settings().get(key);
    }

    /** A transaction manager without a resource, as an app on the in-memory store might have. */
    static final class NoResources extends AbstractPlatformTransactionManager {
        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, org.springframework.transaction.TransactionDefinition definition) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }
}
