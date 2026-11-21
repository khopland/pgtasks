package io.github.khopland.pgtasks.spring;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.LimitOverride;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.engine.EffectiveSettings;
import io.github.khopland.pgtasks.engine.Rate;
import io.github.khopland.pgtasks.engine.RateLimit;
import io.github.khopland.pgtasks.engine.RateLimits;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.VersionConflictException;
import io.github.khopland.pgtasks.spi.TaskStore;
import org.springframework.boot.actuate.endpoint.SecurityContext;
import org.springframework.boot.actuate.endpoint.Show;
import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.jspecify.annotations.Nullable;

/**
 * {@code /actuator/tasks}: the topics, their settings and dead letters, and the rate limits,
 * with the operator's changes. Everything here is also library API, on {@link TaskEngine#admin()} and {@link TaskEngine#limits()}.
 *
 * <pre>
 * GET    /actuator/tasks                                   topics by module, limits, this node
 * GET    /actuator/tasks/topics/{topic}                    status, counts, settings and where each came from, schedules
 * GET    /actuator/tasks/topics/{topic}/dead-letters?after=&amp;limit=
 * GET    /actuator/tasks/topics/{topic}/failing                what is failing, grouped by error
 * POST   /actuator/tasks/topics/{topic}/pause | resume | create
 * POST   /actuator/tasks/topics/{topic}/revive | discard   {"ids": "1,2"}; or {"error": "java.net.ConnectException",
 *                                                        "failedFrom": "...", "failedUntil": "..."}; or {"all": "true"}
 * POST   /actuator/tasks/topics/{topic}/purge              {"outcome": "succeeded", "before": "2026-01-01T00:00:00Z"}
 * GET    /actuator/tasks/tasks/{id}
 * GET    /actuator/tasks/limits/{limit}
 * POST   /actuator/tasks/limits/{limit}                    {"rate": "20/s", "burst": 10, "until": "PT2H", "version": 3}
 * DELETE /actuator/tasks/limits/{limit}?version=3          back to the configured values
 * DELETE /actuator/tasks/topics/{topic}                     a topic no longer used, with its history
 * </pre>
 *
 * Limit changes are optimistic: a limit's {@code version} works as its ETag, and a change that
 * passes one works as If-Match. A change based on an older version gets 409 Conflict, so two
 * operators can't overwrite each other unknowingly. (Actuator endpoints can't read or set
 * headers, so the version travels in the body.) Without a version, a change applies
 * regardless. Request bodies are flat JSON objects of strings and numbers, as Actuator reads them,
 * so a list of ids is one comma-separated string.
 *
 * <p>Payloads, outputs and error messages often hold personal data, so they are shown only as
 * {@code management.endpoint.tasks.show-values} allows, like Boot's {@code env} endpoint:
 * {@code never} (the default), {@code when-authorized} (with {@code management.endpoint.tasks.roles})
 * or {@code always}. Hidden, they read {@value #HIDDEN}; an error keeps its exception type, so
 * operators can still tell failures apart and revive them by type.
 */
@WebEndpoint(id = "tasks")
public class TaskEndpoint {

    static final int CONFLICT = 409;
    static final String HIDDEN = "******";
    private static final int DEAD_LETTER_PAGE = 50;
    /**
     * An exception type, as errors are grouped by, or an expiry: shown even when values are
     * hidden. A message, which may hold personal data, doesn't look like one.
     */
    private static final java.util.regex.Pattern TYPE = java.util.regex.Pattern.compile("[A-Za-z_$][\\w$]*(\\.[A-Za-z_$][\\w$]*)+|expired");

    private final TaskEngine engine;
    private final Show show;
    private final Set<String> roles;

    /** Shows no payloads, outputs or error messages. */
    public TaskEndpoint(TaskEngine engine) {
        this(engine, Show.NEVER, Set.of());
    }

    /** @param roles the roles that may see values when {@code show} is {@code WHEN_AUTHORIZED} */
    public TaskEndpoint(TaskEngine engine, Show show, Set<String> roles) {
        this.engine = engine;
        this.show = show;
        this.roles = Set.copyOf(roles);
    }

    @ReadOperation
    public Map<String, Object> overview() throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        Map<String, List<String>> modules = new TreeMap<>();
        List<Map<String, Object>> topics = engine.admin().topics().stream().map(status -> {
            modules.computeIfAbsent(status.module() == null ? "(other nodes)" : status.module(), ignored -> new java.util.ArrayList<>())
                .add(status.name());
            return topic(status);
        }).toList();
        body.put("modules", modules);
        body.put("topics", topics);
        body.put("limits", engine.limits().list().stream().map(TaskEndpoint::limit).toList());
        body.put("node", node());
        return body;
    }

    @ReadOperation
    public WebEndpointResponse<Object> read(@Selector String kind, @Selector String name, @Nullable SecurityContext security) throws Exception {
        boolean shown = show.isShown(security == null ? SecurityContext.NONE : security, roles);
        return switch (kind) {
            case "topics" -> found(status(name).map(status -> {
                Map<String, Object> body = topic(status);
                engine.effectiveSettings().stream().filter(settings -> settings.topic().equals(name)).findFirst()
                    .ifPresent(settings -> body.put("settings", settings(settings)));
                body.put("recurring", schedules(name));
                return body;
            }));
            case "limits" -> found(engine.limits().find(name).map(TaskEndpoint::limit));
            case "tasks" -> found(number(name).flatMap(this::task).map(view -> task(view, shown)));
            default -> notFound();
        };
    }

    @ReadOperation
    public WebEndpointResponse<Object> readPart(@Selector String kind, @Selector String name, @Selector String part,
                                                @Nullable Long after, @Nullable Integer limit, @Nullable SecurityContext security) throws Exception {
        boolean shown = show.isShown(security == null ? SecurityContext.NONE : security, roles);
        if (!kind.equals("topics") || !Set.of("dead-letters", "failing").contains(part)) {
            return notFound();
        }
        if (status(name).isEmpty()) {
            return notFound();
        }
        if (part.equals("failing")) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("failing", engine.admin().failing(name, limit == null ? 50 : Math.clamp(limit, 1, 1_000)).stream().map(group -> {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("error", shown || TYPE.matcher(group.error()).matches() ? group.error() : HIDDEN);
                entry.put("tasks", group.tasks());
                entry.put("oldestFailedAt", text(group.oldestFailedAt()));
                entry.put("example", group.example());
                entry.put("exampleMessage", shown || group.exampleMessage() == null ? group.exampleMessage() : HIDDEN);
                return entry;
            }).toList());
            return new WebEndpointResponse<>(body);
        }
        int size = limit == null ? DEAD_LETTER_PAGE : Math.clamp(limit, 1, 1_000);
        List<TaskView> page = engine.admin().deadLetters(name, after == null ? 0 : after, size);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("deadLetters", page.stream().map(view -> task(view, shown)).toList());
        // The next page starts after the last id of a full one.
        body.put("next", page.size() == size ? page.getLast().id() : null);
        return new WebEndpointResponse<>(body);
    }

    /** Topic actions: pause, resume, create, revive, discard, purge. */
    @WriteOperation
    public WebEndpointResponse<Object> act(@Selector String kind, @Selector String name, @Selector String action,
                                           @Nullable String ids, @Nullable String outcome, @Nullable String before,
                                           @Nullable String error, @Nullable String failedFrom, @Nullable String failedUntil,
                                           @Nullable String all) throws Exception {
        if (!kind.equals("topics")) {
            return notFound();
        }
        if (!action.equals("create") && status(name).isEmpty()) {
            return notFound();
        }
        List<Long> taskIds;
        try {
            taskIds = ids == null || ids.isBlank() ? null
                : java.util.Arrays.stream(ids.split(",")).map(String::strip).map(Long::valueOf).toList();
        } catch (NumberFormatException e) {
            return badRequest("ids is a comma-separated list of task ids, such as \"12,13\"");
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", name);
        body.put("action", action);
        switch (action) {
            case "pause" -> engine.admin().pause(name);
            case "resume" -> engine.admin().resume(name);
            case "create" -> engine.admin().createTopic(name);
            case "revive", "discard" -> {
                DeadLetterQuery query;
                try {
                    query = deadLetterQuery(taskIds, error, failedFrom, failedUntil, all);
                } catch (IllegalArgumentException | java.time.format.DateTimeParseException e) {
                    return badRequest(e.getMessage());
                }
                body.put(action.equals("revive") ? "revived" : "discarded",
                    action.equals("revive") ? engine.admin().revive(name, query) : engine.admin().discard(name, query));
            }
            case "purge" -> {
                if (outcome == null || before == null) {
                    return badRequest("purge needs an outcome (succeeded, filtered, failed, redundant or recreated) and a time to purge before");
                }
                Outcome purged;
                Instant until;
                try {
                    purged = Outcome.valueOf(outcome.toUpperCase(Locale.ROOT));
                    until = Instant.parse(before);
                } catch (RuntimeException e) {
                    return badRequest(e.getMessage());
                }
                body.put("purged", engine.admin().purge(name, purged, until));
            }
            default -> {
                return notFound();
            }
        }
        return new WebEndpointResponse<>(body);
    }

    /** Overrides a rate limit on every node. */
    @WriteOperation
    public WebEndpointResponse<Object> override(@Selector String kind, @Selector String name, @Nullable String rate, @Nullable Integer burst,
                                                @Nullable String until, @Nullable Long version, SecurityContext security) throws Exception {
        if (!kind.equals("limits")) {
            return notFound();
        }
        if (engine.limits().find(name).isEmpty()) {
            return notFound();
        }
        try {
            return new WebEndpointResponse<>(limit(engine.limits().override(name, rate == null ? null : Rate.parse(rate), burst,
                until == null ? null : Duration.parse(until), who(security), version)));
        } catch (VersionConflictException e) {
            return new WebEndpointResponse<>(error(e.getMessage()), CONFLICT);
        } catch (IllegalArgumentException | java.time.format.DateTimeParseException e) {
            return badRequest(e.getMessage());
        }
    }

    /**
     * Resets a rate limit to its configured values on every node; or deletes a topic no longer
     * used, with its history (409 while it has unfinished tasks).
     */
    @DeleteOperation
    public WebEndpointResponse<Object> delete(@Selector String kind, @Selector String name, @Nullable Long version, SecurityContext security)
        throws Exception {
        if (kind.equals("topics")) {
            if (status(name).isEmpty()) {
                return notFound();
            }
            try {
                return new WebEndpointResponse<>(Map.of("topic", name, "deletedTasks", engine.admin().deleteTopic(name)));
            } catch (IllegalStateException e) {
                return new WebEndpointResponse<>(error(e.getMessage()), CONFLICT);
            }
        }
        if (!kind.equals("limits") || engine.limits().find(name).isEmpty()) {
            return notFound();
        }
        try {
            return new WebEndpointResponse<>(limit(engine.limits().reset(name, who(security), version)));
        } catch (VersionConflictException e) {
            return new WebEndpointResponse<>(error(e.getMessage()), CONFLICT);
        }
    }

    /**
     * Ids, or a filter, or all of them, but only when asked for in so many words: one stray call
     * with an empty body would otherwise finish every dead letter of a topic.
     */
    private static DeadLetterQuery deadLetterQuery(@Nullable List<Long> taskIds, @Nullable String error, @Nullable String failedFrom,
                                                   @Nullable String failedUntil, @Nullable String all) {
        boolean filtered = error != null || failedFrom != null || failedUntil != null;
        if (taskIds != null) {
            return DeadLetterQuery.ids(taskIds);
        }
        if (!filtered && !"true".equalsIgnoreCase(all)) {
            throw new IllegalArgumentException("Name the dead letters: \"ids\", or \"error\", \"failedFrom\" and \"failedUntil\", or \"all\": \"true\" for every one");
        }
        return DeadLetterQuery.ALL.withError(error)
            .failedBetween(failedFrom == null ? null : Instant.parse(failedFrom), failedUntil == null ? null : Instant.parse(failedUntil));
    }

    private Map<String, Object> node() throws Exception {
        var health = engine.health();
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("name", health.node());
        node.put("id", health.nodeId());
        node.put("running", health.running());
        node.put("leader", health.leader());
        node.put("processing", engine.processing().stream().sorted().toList());
        return node;
    }

    private List<Map<String, Object>> schedules(String topic) {
        return engine.admin().schedules().stream().filter(schedule -> schedule.topic().equals(topic)).map(schedule -> {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("name", schedule.name());
            body.put("cron", schedule.cron());
            body.put("zone", schedule.zone());
            body.put("identifier", schedule.identifier());
            body.put("insertion", schedule.insertion().name().toLowerCase(Locale.ROOT));
            body.put("nextRunAt", text(schedule.nextRunAt()));
            body.put("lastRunAt", text(schedule.lastRunAt()));
            return body;
        }).toList();
    }

    private Optional<io.github.khopland.pgtasks.admin.TopicStatus> status(String name) {
        return engine.admin().topic(name);
    }

    private Optional<TaskView> task(long id) {
        return engine.admin().find(id);
    }

    private static Map<String, Object> topic(io.github.khopland.pgtasks.admin.TopicStatus status) {
        Map<String, Object> topic = new LinkedHashMap<>();
        topic.put("name", status.name());
        topic.put("module", status.module());
        topic.put("paused", status.paused());
        topic.put("processing", status.processing());
        topic.put("breaker", status.breakerOpen() ? "open" : "closed");
        QueueCounts counts = status.counts();
        if (counts != null) {
            Map<String, Object> queue = new LinkedHashMap<>();
            queue.put("scheduled", counts.scheduled());
            queue.put("retrying", counts.retrying());
            queue.put("ready", counts.ready());
            queue.put("active", counts.active());
            queue.put("suspended", counts.suspended());
            queue.put("deadLetters", counts.failed());
            queue.put("due", counts.due());
            queue.put("failing", counts.failing());
            queue.put("oldestFailingAt", text(counts.oldestFailingAt()));
            queue.put("oldestReadyAt", text(counts.oldestReadyAt()));
            queue.put("countedAt", text(status.countedAt()));
            topic.put("queue", queue);
        }
        return topic;
    }

    private static Map<String, Object> settings(EffectiveSettings settings) {
        Map<String, Object> values = new TreeMap<>();
        settings.settings().forEach((key, setting) -> values.put(key, Map.of("value", setting.value(),
            "source", setting.source().name().toLowerCase(Locale.ROOT).replace('_', '-'))));
        return values;
    }

    static Map<String, Object> limit(RateLimits.Limit limit) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", limit.name());
        body.put("configured", rate(limit.configured()));
        if (limit.configured() != null && !limit.configured().windows().isEmpty()) {
            body.put("windows", limit.configured().windows().stream().map(window -> Map.of(
                "from", window.from().expression(), "zone", window.from().zone().getId(), "rate", window.rate().toString())).toList());
        }
        body.put("effective", rate(limit.effective()));
        LimitOverride override = limit.override();
        if (override != null && (override.perSecond() != null || override.burst() != null)) {
            Map<String, Object> changed = new LinkedHashMap<>();
            changed.put("rate", override.perSecond() == null ? null : Rate.perSecond(override.perSecond()).toString());
            changed.put("burst", override.burst());
            changed.put("until", text(override.until()));
            changed.put("updatedAt", text(override.updatedAt()));
            changed.put("updatedBy", override.updatedBy());
            body.put("override", changed);
        }
        body.put("version", limit.version());
        return body;
    }

    private static @Nullable Map<String, Object> rate(@Nullable RateLimit limit) {
        if (limit == null) {
            return null;
        }
        Map<String, Object> rate = new LinkedHashMap<>();
        rate.put("rate", Rate.perSecond(limit.perSecond()).toString());
        rate.put("burst", limit.burst());
        return rate;
    }

    private static Map<String, Object> task(TaskView view, boolean shown) {
        Map<String, Object> task = new LinkedHashMap<>();
        task.put("id", view.id());
        task.put("topic", view.topic());
        task.put("identifier", view.identifier());
        task.put("state", view.state());
        task.put("finished", view.finished());
        task.put("attempt", view.attempt());
        task.put("runAt", text(view.runAt()));
        task.put("priority", view.priority());
        task.put("node", view.nodeId());
        task.put("reference", view.reference());
        task.put("parentId", view.parentId());
        task.put("rootId", view.rootId());
        task.put("payloadType", view.payloadType());
        task.put("payload", shown || view.payload() == null ? view.payload() : HIDDEN);
        task.put("output", shown || view.output() == null ? view.output() : HIDDEN);
        task.put("error", shown || view.error() == null ? view.error() : errorType(view.error()));
        task.put("failures", view.failures());
        task.put("firstFailedAt", text(view.firstFailedAt()));
        task.put("createdAt", text(view.createdAt()));
        task.put("completedAt", text(view.completedAt()));
        return task;
    }

    /** A stored error without its message and stack: {"type": "..."}, or hidden if it has no type. */
    private static String errorType(String error) {
        java.util.regex.Matcher type = java.util.regex.Pattern.compile("\"type\"\\s*:\\s*\"([^\"]*)\"").matcher(error);
        return type.find() ? "{\"type\":\"" + type.group(1) + "\",\"message\":\"" + HIDDEN + "\"}" : HIDDEN;
    }

    private static @Nullable String text(@Nullable Instant instant) {
        return instant == null ? null : instant.toString();
    }

    private static String who(SecurityContext security) {
        return security != null && security.getPrincipal() != null ? security.getPrincipal().getName() : "actuator";
    }

    private static Optional<Long> number(String text) {
        try {
            return Optional.of(Long.parseLong(text));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static WebEndpointResponse<Object> found(Optional<?> body) {
        return body.<WebEndpointResponse<Object>>map(WebEndpointResponse::new).orElseGet(TaskEndpoint::notFound);
    }

    private static WebEndpointResponse<Object> notFound() {
        return new WebEndpointResponse<>(WebEndpointResponse.STATUS_NOT_FOUND);
    }

    private static WebEndpointResponse<Object> badRequest(@Nullable String message) {
        return new WebEndpointResponse<>(error(message), WebEndpointResponse.STATUS_BAD_REQUEST);
    }

    private static Map<String, Object> error(@Nullable String message) {
        return Map.of("error", message == null ? "invalid request" : message);
    }
}
