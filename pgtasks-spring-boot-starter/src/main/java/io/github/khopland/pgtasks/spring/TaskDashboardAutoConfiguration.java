package io.github.khopland.pgtasks.spring;

import java.util.LinkedHashMap;
import java.util.Map;

import io.github.khopland.pgtasks.engine.TaskEngine;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
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
import org.springframework.util.ClassUtils;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;
import tools.jackson.databind.json.JsonMapper;

/**
 * With {@code pgtasks.dashboard.enabled=true}, in a Spring MVC application with Actuator: the
 * dashboard at {@code pgtasks.dashboard.path} ({@code /pgtasks} by default). By default it reads
 * and changes everything through the {@code tasks} endpoint, so it needs that endpoint exposed,
 * and has exactly the endpoint's security. With {@code pgtasks.dashboard.api=dashboard} it serves
 * the same operations itself at {@code <path>/api}, for when Actuator's port isn't reachable.
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
@EnableConfigurationProperties({TaskDashboardProperties.class, TaskEndpointProperties.class})
public class TaskDashboardAutoConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskDashboardAutoConfiguration.class);

    @Bean
    SimpleUrlHandlerMapping pgtasksDashboardHandlerMapping(TaskDashboardProperties properties, ObjectProvider<PathMappedEndpoints> endpoints,
                                                           Environment environment, TaskEngine engine, TaskEndpointProperties endpointProperties,
                                                           BeanFactory beans) {
        Map<String, Object> urls = new LinkedHashMap<>();
        String endpoint;
        if (properties.api() == TaskDashboardProperties.Api.DASHBOARD) {
            endpoint = properties.path() + "/api";
            Object api = DashboardApi.create(endpoint, engine, endpointProperties, beans);
            urls.put(endpoint, api);
            urls.put(endpoint + "/**", api);
            if (!ClassUtils.isPresent("org.springframework.security.web.SecurityFilterChain", TaskDashboardAutoConfiguration.class.getClassLoader())) {
                LOGGER.warn("The pgtasks dashboard serves its API at {}, which changes tasks, and this application has no Spring Security: "
                    + "protect {}/** before exposing it", endpoint, properties.path());
            }
        } else {
            endpoint = properties.endpoint() != null ? properties.endpoint() : endpointPath(endpoints.getIfAvailable());
            if (endpoint == null) {
                LOGGER.warn("The pgtasks dashboard at {} needs the tasks endpoint: management.endpoints.web.exposure.include=health,tasks, "
                    + "or pgtasks.dashboard.api=dashboard to serve its API next to the page", properties.path());
            } else if (properties.endpoint() == null && ManagementPortType.get(environment) == ManagementPortType.DIFFERENT) {
                LOGGER.warn("Actuator runs on its own port, so the pgtasks dashboard at {} can't reach {}: set "
                    + "pgtasks.dashboard.api=dashboard to serve its API next to the page", properties.path(), endpoint);
            }
        }
        TaskDashboard dashboard = new TaskDashboard(properties.path(), endpoint, TaskDashboard.class.getClassLoader());
        urls.put(properties.path(), dashboard);
        urls.put(properties.path() + "/**", dashboard);
        // Ahead of controllers and static resources, which would otherwise answer /** first.
        return new SimpleUrlHandlerMapping(urls, Ordered.HIGHEST_PRECEDENCE + 10);
    }

    /** Kept apart, so Jackson is loaded only when the API is served. */
    private static final class DashboardApi {
        static Object create(String path, TaskEngine engine, TaskEndpointProperties properties, BeanFactory beans) {
            if (!ClassUtils.isPresent("tools.jackson.databind.json.JsonMapper", DashboardApi.class.getClassLoader())) {
                throw new IllegalStateException("pgtasks.dashboard.api=dashboard needs Jackson 3 (tools.jackson.core:jackson-databind), "
                    + "as Spring Boot's web starter brings");
            }
            JsonMapper json = beans.getBeanProvider(JsonMapper.class).getIfAvailable(() -> JsonMapper.builder().build());
            return new TaskDashboardApi(path, new TaskEndpoint(engine, properties.showValues(), properties.roles()), json);
        }
    }

    private static @Nullable String endpointPath(@Nullable PathMappedEndpoints endpoints) {
        return endpoints == null ? null : endpoints.getPath(EndpointId.of("tasks"));
    }
}
