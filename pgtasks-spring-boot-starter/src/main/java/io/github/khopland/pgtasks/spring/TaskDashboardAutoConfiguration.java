package io.github.khopland.pgtasks.spring;

import java.util.LinkedHashMap;
import java.util.Map;

import io.github.khopland.pgtasks.engine.TaskEngine;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.web.server.ManagementPortType;
import org.springframework.boot.actuate.endpoint.EndpointId;
import org.springframework.boot.actuate.endpoint.web.PathMappedEndpoints;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;

/**
 * With {@code pgtasks.dashboard.enabled=true}, in a Spring MVC application with Actuator: the
 * dashboard at {@code pgtasks.dashboard.path} ({@code /pgtasks} by default). It reads and changes
 * everything through the {@code tasks} endpoint, so it needs that endpoint exposed, and has
 * exactly the endpoint's security.
 */
@AutoConfiguration(after = TaskActuatorAutoConfiguration.class,
    afterName = "org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointAutoConfiguration")
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(name = {
    "org.springframework.web.servlet.handler.SimpleUrlHandlerMapping",
    "org.springframework.boot.actuate.endpoint.web.PathMappedEndpoints",
    "org.springframework.boot.actuate.autoconfigure.web.server.ManagementPortType"
})
@ConditionalOnBooleanProperty("pgtasks.dashboard.enabled")
@ConditionalOnBean(TaskEngine.class)
@EnableConfigurationProperties(TaskDashboardProperties.class)
public class TaskDashboardAutoConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskDashboardAutoConfiguration.class);

    @Bean
    SimpleUrlHandlerMapping pgtasksDashboardHandlerMapping(TaskDashboardProperties properties, ObjectProvider<PathMappedEndpoints> endpoints,
                                                           Environment environment) {
        String endpoint = properties.endpoint() != null ? properties.endpoint() : endpointPath(endpoints.getIfAvailable());
        if (endpoint == null) {
            LOGGER.warn("The pgtasks dashboard at {} needs the tasks endpoint: management.endpoints.web.exposure.include=health,tasks",
                properties.path());
        } else if (properties.endpoint() == null && ManagementPortType.get(environment) == ManagementPortType.DIFFERENT) {
            LOGGER.warn("Actuator runs on its own port, so the pgtasks dashboard at {} can't reach {}: "
                + "set pgtasks.dashboard.endpoint to its URL (and allow it with CORS)", properties.path(), endpoint);
        }
        TaskDashboard dashboard = new TaskDashboard(properties.path(), endpoint, TaskDashboard.class.getClassLoader());
        Map<String, Object> urls = new LinkedHashMap<>();
        urls.put(properties.path(), dashboard);
        urls.put(properties.path() + "/**", dashboard);
        // Ahead of controllers and static resources, which would otherwise answer /** first.
        return new SimpleUrlHandlerMapping(urls, Ordered.HIGHEST_PRECEDENCE + 10);
    }

    private static @Nullable String endpointPath(@Nullable PathMappedEndpoints endpoints) {
        return endpoints == null ? null : endpoints.getPath(EndpointId.of("tasks"));
    }
}
