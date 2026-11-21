package io.github.khopland.pgtasks.spring;

import io.github.khopland.pgtasks.engine.TaskEngine;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * With Spring Boot Actuator: the {@code tasks} endpoint ({@link TaskEndpoint}, exposed like any
 * other, for example with {@code management.endpoints.web.exposure.include=health,tasks}) and the
 * {@code tasks} health indicator ({@link TaskHealthIndicator}). The endpoint hides payloads and
 * error messages unless {@code management.endpoint.tasks.show-values} says otherwise.
 */
@AutoConfiguration(after = TaskAutoConfiguration.class)
@ConditionalOnBean(TaskEngine.class)
public class TaskActuatorAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = {
        "org.springframework.boot.actuate.endpoint.annotation.Endpoint",
        "org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint"
    })
    @org.springframework.boot.context.properties.EnableConfigurationProperties(TaskEndpointProperties.class)
    static class Endpoint {
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnAvailableEndpoint
        TaskEndpoint taskEndpoint(TaskEngine engine, TaskEndpointProperties properties) {
            return new TaskEndpoint(engine, properties.showValues(), properties.roles());
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = {
        "org.springframework.boot.health.contributor.HealthIndicator",
        "org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator"
    })
    static class Health {
        @Bean
        @ConditionalOnMissingBean(name = "tasksHealthIndicator")
        @ConditionalOnEnabledHealthIndicator("tasks")
        TaskHealthIndicator tasksHealthIndicator(TaskEngine engine, TaskProperties properties) {
            return new TaskHealthIndicator(engine, properties.enabled());
        }
    }
}
