package org.example.task2.spring;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The {@code task.*} settings. Topics, limits and their defaults are code, in
 * {@link org.example.task2.engine.TaskConfigurer} and {@link org.example.task2.engine.TaskGlobals}
 * beans; these properties hold only what differs per environment.
 *
 * <pre>
 * task:
 *   topics:
 *     assessment: { concurrency: 16, retry.max-attempts: 10 }
 *   limits:
 *     folkeregister: { rate: 400/s }
 * </pre>
 *
 * A topic or limit that no configurer defines, or an unknown setting, fails startup.
 *
 * @param store           where tasks live: {@code postgres} (the default, through the application's
 *                        DataSource) or {@code memory}, for local runs and tests. Memory is never
 *                        chosen on its own: without a DataSource, startup fails
 * @param schemaMode      how the Postgres store treats the tasks schema at startup: {@code migrate}
 *                        (the built-in runner), {@code validate} (Flyway, Liquibase or a DBA
 *                        pipeline applies it) or {@code none}
 * @param node            this node's name in tasks.node; by default {@code spring.application.name}
 * @param notifications   wake workers through LISTEN/NOTIFY; off relies on polling alone
 * @param shutdownTimeout how long running batches may finish when the application stops; what is
 *                        left is handed back without counting the attempt
 * @param topics          per topic: concurrency, batch-size, ordering, poll-interval,
 *                        batch-deadline, retry.max-attempts, retain.&lt;outcome&gt;
 * @param limits          per rate limit: rate (such as 400/s) and burst
 * @param testing         adds a {@code TaskTester} bean, which records every push, for tests;
 *                        Spring Modulith module tests get one without it
 */
@ConfigurationProperties("task")
public record TaskProperties(
    @DefaultValue("postgres") Store store,
    @DefaultValue("migrate") String schemaMode,
    String node,
    @DefaultValue("true") boolean notifications,
    @DefaultValue("30s") Duration shutdownTimeout,
    Map<String, Map<String, String>> topics,
    Map<String, Map<String, String>> limits,
    @DefaultValue("false") boolean testing
) {

    public enum Store { POSTGRES, MEMORY }

    public TaskProperties {
        topics = topics == null ? Map.of() : topics;
        limits = limits == null ? Map.of() : limits;
    }

    /** As the engine builder takes them: {@code topics.<topic>.<setting>} and {@code limits.<limit>.<setting>}. */
    public Map<String, String> overrides() {
        Map<String, String> overrides = new TreeMap<>();
        topics.forEach((topic, settings) -> settings.forEach((key, value) -> overrides.put("topics." + topic + "." + key, value)));
        limits.forEach((limit, settings) -> settings.forEach((key, value) -> overrides.put("limits." + limit + "." + key, value)));
        return overrides;
    }
}
