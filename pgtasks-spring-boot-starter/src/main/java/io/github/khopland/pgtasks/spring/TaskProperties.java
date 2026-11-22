package io.github.khopland.pgtasks.spring;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The {@code pgtasks.*} settings. Topics, limits and their defaults are code, in
 * {@link io.github.khopland.pgtasks.engine.TaskConfigurer} and {@link io.github.khopland.pgtasks.engine.TaskGlobals}
 * beans; these properties hold only what differs per environment.
 *
 * <pre>
 * pgtasks:
 *   topics:
 *     assessment: { concurrency: 16, retry.give-up-after: 2d }
 *   limits:
 *     folkeregister: { rate: 400/s }
 * </pre>
 *
 * A topic or limit that no configurer defines, or an unknown setting, fails startup.
 *
 * @param enabled         false for a node that only pushes tasks: it applies the schema and
 *                        registers its configured topics, so its pushes work whichever node starts
 *                        first, but it processes nothing, runs no housekeeping and listens for no
 *                        notifications. Other nodes, with it true, run the topics
 * @param store           where tasks live: {@code postgres} (the default, through the application's
 *                        DataSource) or {@code memory}, for local runs and tests. Memory is never
 *                        chosen on its own: without a DataSource, startup fails
 * @param schemaMode      how the Postgres store treats the pgtasks schema at startup: {@code migrate}
 *                        (the built-in runner), {@code validate} (Flyway, Liquibase or a DBA
 *                        pipeline applies it) or {@code none}, which leaves it to the application
 * @param schema          the Postgres schema the tasks live in, {@code pgtasks} by default: a plain
 *                        lowercase identifier. Applications sharing a database each need their own.
 *                        With Flyway or Liquibase, pass the same name to them (docs/operations.md)
 * @param node            this node's name in pgtasks.node; by default {@code spring.application.name}
 * @param notifications   wake workers through LISTEN/NOTIFY; off relies on polling alone
 * @param shutdownTimeout how long running batches may finish when the application stops; what is
 *                        left is handed back without counting the attempt
 * @param topics          per topic: concurrency, batch-size, ordering, poll-interval,
 *                        batch-deadline, retry.max-attempts (a number or forever),
 *                        retry.give-up-after (such as 7d, or never), retry.first-backoff,
 *                        retry.max-backoff, retain.&lt;outcome&gt;
 * @param limits          per rate limit: rate (such as 400/s) and burst
 * @param testing         adds a {@code TaskTester} bean, which records every push, for tests;
 *                        Spring Modulith module tests get one without it
 * @param version         the application's version, for topics that revive their dead letters
 *                        on a new version; by default the build's, from {@code BuildProperties}
 *                        (the build-info goal of the Spring Boot Maven or Gradle plugin)
 * @param mdcHeaders      MDC keys that follow the work, such as a correlation id: each one the MDC
 *                        has when a task is pushed is stored as a header of the same name, and is
 *                        in the MDC again while a handler works on the task, so downstream pushes
 *                        carry it too ({@code TaskObserver.mdc})
 */
@ConfigurationProperties("pgtasks")
@SuppressWarnings("InvalidParam") // {@code none} is a schema mode, not a misspelt {@code node}
public record TaskProperties(
    @DefaultValue("true") boolean enabled,
    @DefaultValue("postgres") Store store,
    @DefaultValue("migrate") String schemaMode,
    @DefaultValue("pgtasks") String schema,
    String node,
    @DefaultValue("true") boolean notifications,
    @DefaultValue("30s") Duration shutdownTimeout,
    Map<String, Map<String, String>> topics,
    Map<String, Map<String, String>> limits,
    @DefaultValue("false") boolean testing,
    String version,
    List<String> mdcHeaders
) {

    public enum Store { POSTGRES, MEMORY }

    public TaskProperties {
        topics = topics == null ? Map.of() : topics;
        limits = limits == null ? Map.of() : limits;
        mdcHeaders = mdcHeaders == null ? List.of() : List.copyOf(mdcHeaders);
    }

    /** As the engine builder takes them: {@code topics.<topic>.<setting>} and {@code limits.<limit>.<setting>}. */
    public Map<String, String> overrides() {
        Map<String, String> overrides = new TreeMap<>();
        topics.forEach((topic, settings) -> settings.forEach((key, value) -> overrides.put("topics." + topic + "." + key, value)));
        limits.forEach((limit, settings) -> settings.forEach((key, value) -> overrides.put("limits." + limit + "." + key, value)));
        return overrides;
    }
}
