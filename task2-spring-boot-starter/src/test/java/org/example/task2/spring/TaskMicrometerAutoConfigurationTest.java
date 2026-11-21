package org.example.task2.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.example.task2.Decision;
import org.example.task2.Pushed;
import org.example.task2.TopicRef;
import org.example.task2.engine.EngineFixture;
import org.example.task2.engine.TaskConfigurer;
import org.example.task2.engine.TaskObserver;
import org.example.task2.micrometer.TaskMetrics;
import org.example.task2.micrometer.TaskObservations;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.micrometer.observation.autoconfigure.ObservationAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class TaskMicrometerAutoConfigurationTest {

    static final TopicRef<String> NOTIFY = TopicRef.of("notify", String.class);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(TaskMicrometerAutoConfiguration.class, TaskAutoConfiguration.class))
        .withPropertyValues("task.store=memory")
        .withUserConfiguration(Notify.class);

    @Configuration(proxyBeanMethods = false)
    static class Notify {
        @Bean
        TaskConfigurer notifyTasks() {
            return tasks -> tasks.topic(NOTIFY, batch -> batch.decide(task -> Decision.success()));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Micrometer {
        @Bean
        ObservationRegistry observationRegistry() {
            return ObservationRegistry.create();
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OwnObserver {
        final List<String> pushedTo = new CopyOnWriteArrayList<>();

        @Bean
        TaskObserver recordingObserver() {
            return new TaskObserver() {
                @Override
                public void pushed(TopicRef<?> topic, List<Pushed> results) {
                    pushedTo.add(topic.name());
                }
            };
        }
    }

    @Test
    void with_micrometer_the_engine_is_observed_and_counted() {
        runner.withUserConfiguration(Micrometer.class).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(TaskObservations.class).hasSingleBean(TaskMetrics.class);
            context.getBean(org.example.task2.Tasks.class).independently().push(NOTIFY, "anna", "hello");

            MeterRegistry meters = context.getBean(MeterRegistry.class);
            EngineFixture.await("the task completed", Duration.ofSeconds(5), () -> meters.find(TaskObservations.COMPLETED)
                .tags("topic", "notify", "result", "succeeded").counter() != null);
            assertThat(meters.get(TaskObservations.PUSHED).tags("topic", "notify", "status", "queued").counter().count()).isEqualTo(1);
        });
    }

    @Test
    void with_actuator_the_queue_gauges_are_bound() {
        runner.withConfiguration(AutoConfigurations.of(ObservationAutoConfiguration.class, MetricsAutoConfiguration.class,
                CompositeMeterRegistryAutoConfiguration.class, SimpleMetricsExportAutoConfiguration.class))
            .run(context -> {
                assertThat(context).hasNotFailed().hasSingleBean(TaskObservations.class).hasSingleBean(TaskMetrics.class);
                assertThat(context.getBean(MeterRegistry.class).find(TaskMetrics.TOPICS).gauge()).isNotNull();
            });
    }

    @Test
    void without_micrometer_nothing_observes() {
        runner.run(context -> assertThat(context).hasNotFailed()
            .doesNotHaveBean(TaskObserver.class).doesNotHaveBean(TaskMetrics.class));
    }

    @Test
    void an_observer_bean_of_the_application_replaces_the_observations() {
        runner.withUserConfiguration(Micrometer.class, OwnObserver.class).run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(TaskObservations.class);
            context.getBean(org.example.task2.Tasks.class).independently().push(NOTIFY, "anna", "hello");
            assertThat(context.getBean(OwnObserver.class).pushedTo).containsExactly("notify");
        });
    }
}
