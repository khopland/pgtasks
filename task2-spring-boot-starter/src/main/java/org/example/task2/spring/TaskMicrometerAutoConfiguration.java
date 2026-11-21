package org.example.task2.spring;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.example.task2.engine.TaskEngine;
import org.example.task2.engine.TaskObserver;
import org.example.task2.micrometer.TaskMetrics;
import org.example.task2.micrometer.TaskObservations;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Metrics and tracing, on when the application has Micrometer set up (Spring Boot Actuator does
 * that): with an {@link ObservationRegistry}, the engine observes every push and batch
 * ({@link TaskObservations}), and with a {@link MeterRegistry} it also counts committed pushes and
 * results and gauges the queues ({@link TaskMetrics}). Tracing follows if a tracer is set up,
 * such as with {@code micrometer-tracing-bridge-otel}. A {@link TaskObserver} bean of the
 * application's own sees everything too, next to these.
 */
@AutoConfiguration(before = TaskAutoConfiguration.class, afterName = {
    "org.springframework.boot.micrometer.observation.autoconfigure.ObservationAutoConfiguration",
    "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
    "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration"
})
@ConditionalOnClass({TaskObservations.class, ObservationRegistry.class})
public class TaskMicrometerAutoConfiguration {

    @Bean
    @ConditionalOnBean(ObservationRegistry.class)
    @ConditionalOnMissingBean
    public TaskObservations taskObservations(ObservationRegistry observations, ObjectProvider<MeterRegistry> meters) {
        return new TaskObservations(observations, meters.getIfUnique());
    }

    /** Bound to the registry by Spring Boot Actuator, like every MeterBinder bean. */
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean
    public TaskMetrics taskMetrics(TaskEngine engine) {
        return new TaskMetrics(engine);
    }
}
