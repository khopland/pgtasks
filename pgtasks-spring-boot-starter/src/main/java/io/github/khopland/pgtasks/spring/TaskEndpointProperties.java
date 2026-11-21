package io.github.khopland.pgtasks.spring;

import java.util.Set;

import org.springframework.boot.actuate.endpoint.Show;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code management.endpoint.tasks.*}: who sees the tasks endpoint's payloads, outputs and error
 * messages, as Boot's own endpoints decide who sees values.
 *
 * @param showValues {@code never} (the default), {@code when-authorized} or {@code always}
 * @param roles      with {@code when-authorized}, the roles that see them; any authenticated
 *                   user if empty
 */
@ConfigurationProperties("management.endpoint.tasks")
public record TaskEndpointProperties(@DefaultValue("never") Show showValues, Set<String> roles) {

    public TaskEndpointProperties {
        roles = roles == null ? Set.of() : Set.copyOf(roles);
    }
}
