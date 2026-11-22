package io.github.khopland.pgtasks.spring;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code pgtasks.dashboard.*}: a web page over the {@code tasks} Actuator endpoint, for Spring MVC
 * applications. It has no data of its own: everything it shows and changes goes through the
 * endpoint, with the endpoint's security and its {@code show-values}.
 *
 * @param enabled  off by default
 * @param path     where the page is served, {@code /pgtasks} by default
 * @param endpoint the tasks endpoint as the browser reaches it, when it isn't
 *                 {@code <context path><actuator base path>/tasks} on the same server, such as
 *                 behind a proxy that rewrites paths
 */
@ConfigurationProperties("pgtasks.dashboard")
public record TaskDashboardProperties(@DefaultValue("false") boolean enabled, @DefaultValue("/pgtasks") String path,
                                      @Nullable String endpoint) {

    public TaskDashboardProperties {
        String trimmed = path.strip();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        if (trimmed.isEmpty() || !trimmed.startsWith("/") || trimmed.contains("*") || trimmed.contains("{")) {
            throw new IllegalArgumentException("pgtasks.dashboard.path is a path of its own, such as /pgtasks, not '" + path + "'");
        }
        path = trimmed;
        endpoint = endpoint == null || endpoint.isBlank() ? null : endpoint.strip();
    }
}
