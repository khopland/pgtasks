package io.github.khopland.pgtasks.spring;

import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.Push;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.engine.EffectiveSettings;
import io.github.khopland.pgtasks.engine.EngineFixture;
import io.github.khopland.pgtasks.engine.TaskConfigurer;
import io.github.khopland.pgtasks.engine.Extension;
import io.github.khopland.pgtasks.engine.ExtensionContext;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskGlobals;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.TestPostgres;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            .withPropertyValues("pgtasks.store=memory");
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
                .topic(ASSESSMENT, batch -> batch.decide(task -> Decision.success()));
        }

        @Bean
        TaskGlobals taskSettings() {
            return globals -> globals.housekeeping(housekeeping -> housekeeping.retain(Outcome.SUCCEEDED, Period.ofDays(90)));
        }
    }

    @Test
    void configurer_beans_are_merged_into_one_engine_that_runs_with_the_context() {
        postgres().withUserConfiguration(Modules.class).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(TaskEngine.class).hasSingleBean(Tasks.class);
            TaskEngine engine = context.getBean(TaskEngine.class);
            assertThat(engine.processing()).containsExactlyInAnyOrder("notify", "assessment");
            assertThat(engine.effectiveSettings()).extracting(EffectiveSettings::topic, EffectiveSettings::module)
                .containsExactlyInAnyOrder(Tuple.tuple("notify", "notifyTasks"),
                    Tuple.tuple("assessment", "assessmentTasks"));
            assertThat(setting(engine, "assessment", "concurrency")).isEqualTo(new EffectiveSettings.Setting("8", EffectiveSettings.Source.MODULE_DEFAULTS));

            context.getBean(Tasks.class).independently().push(NOTIFY, "anna", "hello");
            EngineFixture.await("the task ran", Duration.ofSeconds(5), () -> context.getBean(Modules.class).handled.contains("hello"));
        });
    }

    @Test
    void task_properties_override_what_the_code_sets() {
        postgres().withUserConfiguration(Modules.class)
            .withPropertyValues("pgtasks.topics.assessment.concurrency=3", "pgtasks.topics.assessment.retry.max-attempts=2",
                "pgtasks.topics.notify.poll-interval=5s", "pgtasks.node=pod-1")
            .run(context -> {
                assertThat(context).hasNotFailed();
                TaskEngine engine = context.getBean(TaskEngine.class);
                assertThat(setting(engine, "assessment", "concurrency")).isEqualTo(new EffectiveSettings.Setting("3", EffectiveSettings.Source.OVERRIDE));
                assertThat(setting(engine, "assessment", "retry").value()).startsWith("2 attempts");
                assertThat(setting(engine, "notify", "poll-interval")).isEqualTo(new EffectiveSettings.Setting("PT5S", EffectiveSettings.Source.OVERRIDE));
                assertThat(database.rows("SELECT name FROM pgtasks.node")).containsExactly(List.of("pod-1"));
            });
    }

    @Test
    void the_schema_property_names_the_schema_and_its_notifications() {
        postgres().withUserConfiguration(Modules.class)
            .withPropertyValues("pgtasks.schema=shop_tasks", "pgtasks.topics.notify.poll-interval=1h")
            .run(context -> {
                assertThat(context).hasNotFailed();
                context.getBean(Tasks.class).independently().push(NOTIFY, "anna", "hello");
                EngineFixture.await("woken through shop_tasks_ready, not by polling", Duration.ofSeconds(5),
                    () -> context.getBean(Modules.class).handled.contains("hello"));
                assertThat(database.rows("SELECT payload FROM shop_tasks.task")).containsExactly(List.of("hello"));
                assertThat(database.count("SELECT count(*) FROM pg_namespace WHERE nspname = 'pgtasks'")).isZero();
            });
        postgres().withUserConfiguration(Modules.class).withPropertyValues("pgtasks.schema=Shop-Tasks").run(context ->
            assertThat(context).hasFailed().getFailure().rootCause().hasMessageContaining("Not a valid schema name for pgtasks: 'Shop-Tasks'"));
    }

    @Test
    void an_override_for_a_topic_no_configurer_defines_fails_startup() {
        postgres().withUserConfiguration(Modules.class).withPropertyValues("pgtasks.topics.asessment.concurrency=3").run(context ->
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
        postgres().withUserConfiguration(Modules.class).withPropertyValues("pgtasks.topics.notify.poll-interval=1h").run(context -> {
            Tasks tasks = context.getBean(Tasks.class);
            TransactionTemplate transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));

            assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
                tasks.push(NOTIFY, "anna", "rolled back");
                assertThat(database.count("SELECT count(*) FROM pgtasks.task")).as("not visible before the commit").isZero();
                throw new IllegalStateException("the application's work failed");
            })).hasMessage("the application's work failed");
            assertThat(database.count("SELECT count(*) FROM pgtasks.task")).isZero();

            transaction.executeWithoutResult(status -> tasks.push(NOTIFY, "anna", "committed"));
            assertThat(database.rows("SELECT payload FROM pgtasks.task")).containsExactly(List.of("committed"));

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
        postgres().withUserConfiguration(Modules.class).withPropertyValues("pgtasks.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            TaskEngine engine = context.getBean(TaskEngine.class);
            assertThat(engine.running()).isFalse();
            TransactionTemplate transaction = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            transaction.executeWithoutResult(status -> context.getBean(Tasks.class).push(NOTIFY, "anna", "pushed by the producer"));

            Thread.sleep(300);
            assertThat(database.count("SELECT count(*) FROM pgtasks.queue WHERE state = 'ready'")).as("nobody processes here").isEqualTo(1);
            assertThat(database.count("SELECT count(*) FROM pgtasks.node")).as("no node row for a producer").isZero();
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
            assertThat(context.getBean(TaskEngine.class).effectiveSettings()).extracting(EffectiveSettings::topic).containsExactly("notify");
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

    @Configuration(proxyBeanMethods = false)
    static class Extensions {
        final List<String> started = new CopyOnWriteArrayList<>();

        @Bean
        @org.springframework.core.annotation.Order(2)
        Extension second() {
            return new Extension() {
                @Override
                public String name() {
                    return "second";
                }

                @Override
                public void install(ExtensionContext context) {
                    context.onStart(() -> started.add("second"));
                }
            };
        }

        @Bean
        @org.springframework.core.annotation.Order(1)
        Extension first() {
            return new Extension() {
                @Override
                public String name() {
                    return "first";
                }

                @Override
                public void install(ExtensionContext context) {
                    context.onStart(() -> started.add("first"));
                }
            };
        }
    }

    @Test
    void extension_beans_are_installed_in_their_order() {
        memory().withUserConfiguration(Extensions.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(TaskEngine.class).extensions()).containsExactly("first", "second");
            assertThat(context.getBean(Extensions.class).started).containsExactly("first", "second");
        });
    }

    @Test
    void stopping_the_context_drains_the_engine() {
        postgres().withUserConfiguration(Modules.class).run(context -> {
            TaskEngine engine = context.getBean(TaskEngine.class);
            context.close();
            assertThat(engine.processing()).isEmpty();
            assertThat(database.count("SELECT count(*) FROM pgtasks.lease")).isZero();
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
    void mdc_headers_carry_mdc_keys_from_the_push_to_the_handler() {
        List<String> seen = new CopyOnWriteArrayList<>();
        memory().withPropertyValues("pgtasks.mdc-headers=korrelasjonsid")
            .withBean(TaskConfigurer.class, () -> tasks -> tasks.eachTask(NOTIFY, task ->
                seen.add(task.task().header("korrelasjonsid").orElse("none") + "/" + MDC.get("korrelasjonsid"))))
            .run(context -> {
                MDC.put("korrelasjonsid", "k-1");
                try {
                    context.getBean(Tasks.class).independently().push(NOTIFY, "anna", "hello");
                } finally {
                    MDC.remove("korrelasjonsid");
                }
                EngineFixture.await("the task ran", Duration.ofSeconds(5), () -> !seen.isEmpty());
                assertThat(seen).containsExactly("k-1/k-1");
            });
    }

    @Test
    void an_engine_of_the_applications_own_wins() {
        memory().withUserConfiguration(Modules.class)
            .withBean(TaskEngine.class, () -> TaskEngine.builder().store(new InMemoryTaskStore()).build())
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
        protected void doBegin(Object transaction, TransactionDefinition definition) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }
}
