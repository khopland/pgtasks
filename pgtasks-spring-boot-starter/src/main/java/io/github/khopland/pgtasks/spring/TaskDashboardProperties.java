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
 * @param api      where the page's data comes from: {@code actuator} (the default), the tasks
 *                 endpoint; or {@code dashboard}, the same operations served under the page's own
 *                 path ({@code <path>/api}) on the application's port, for when Actuator runs on a
 *                 port the browser can't reach, as in Kubernetes. Then the application's own
 *                 security must protect {@code <path>/**}
 */
@ConfigurationProperties("pgtasks.dashboard")
public record TaskDashboardProperties(@DefaultValue("false") boolean enabled, @DefaultValue("/pgtasks") String path,
                                      @Nullable String endpoint, @DefaultValue("actuator") Api api) {

    /** Where the page's data comes from. */
    public enum Api {
        /** The tasks Actuator endpoint, with Actuator's exposure and security. */
        ACTUATOR,
        /** The same operations at {@code <path>/api}, next to the page, on the application's port. */
        DASHBOARD
    }

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
        api = api == null ? Api.ACTUATOR : api;
        if (endpoint != null && api == Api.DASHBOARD) {
            throw new IllegalArgumentException("pgtasks.dashboard.endpoint points the page at an Actuator endpoint, "
                + "and pgtasks.dashboard.api=dashboard serves the API next to the page: set one of them");
        }
    }
}
