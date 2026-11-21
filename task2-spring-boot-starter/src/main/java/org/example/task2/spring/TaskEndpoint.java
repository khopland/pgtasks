package org.example.task2.spring;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.example.task2.Outcome;
import org.example.task2.engine.EffectiveSettings;
import org.example.task2.engine.Rate;
import org.example.task2.engine.RateLimit;
import org.example.task2.engine.RateLimits;
import org.example.task2.engine.TaskEngine;
import org.example.task2.engine.VersionConflictException;
import org.example.task2.spi.TaskStore;
import org.springframework.boot.actuate.endpoint.SecurityContext;
import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.jspecify.annotations.Nullable;

/**
 * {@code /actuator/tasks}: the topics, their settings and dead letters, and the rate limits,
 * with the operator's changes. Everything here is also library API on {@link TaskEngine}.
 *
 * <pre>
 * GET    /actuator/tasks                                   topics by module, limits, this node
 * GET    /actuator/tasks/topics/{topic}                    status, counts, settings and where each came from, schedules
 * GET    /actuator/tasks/topics/{topic}/dead-letters?after=&amp;limit=
 * GET    /actuator/tasks/topics/{topic}/failing                what is failing, grouped by error
 * POST   /actuator/tasks/topics/{topic}/pause | resume | create
 * POST   /actuator/tasks/topics/{topic}/revive | discard   {"ids": "1,2"}, or {} for all
 * POST   /actuator/tasks/topics/{topic}/purge              {"outcome": "succeeded", "before": "2026-01-01T00:00:00Z"}
 * GET    /actuator/tasks/tasks/{id}
 * GET    /actuator/tasks/limits/{limit}
 * POST   /actuator/tasks/limits/{limit}                    {"rate": "20/s", "burst": 10, "until": "PT2H", "version": 3}
 * DELETE /actuator/tasks/limits/{limit}?version=3          back to the configured values
 * </pre>
 *
 * Limit changes are optimistic: a limit's {@code version} works as its ETag, and a change that
 * passes one works as If-Match. A change based on an older version gets 409 Conflict, so two
 * operators can't overwrite each other unknowingly. (Actuator endpoints can't read or set
 * headers, so the version travels in the body.) Without a version, a change applies
 * regardless. Request bodies are flat JSON objects of strings and numbers, as Actuator reads them,
 * so a list of ids is one comma-separated string.
 */
@WebEndpoint(id = "tasks")
public class TaskEndpoint {

    static final int CONFLICT = 409;
    private static final int DEAD_LETTER_PAGE = 50;

    private final TaskEngine engine;

    public TaskEndpoint(TaskEngine engine) {
        this.engine = engine;
    }

    @ReadOperation
    public Map<String, Object> overview() throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        Map<String, List<String>> modules = new TreeMap<>();
        List<Map<String, Object>> topics = engine.topicStatus().stream().map(status -> {
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
    public WebEndpointResponse<Object> read(@Selector String kind, @Selector String name) throws Exception {
        return switch (kind) {
            case "topics" -> found(status(name).map(status -> {
                Map<String, Object> body = topic(status);
                engine.effectiveSettings().stream().filter(settings -> settings.topic().equals(name)).findFirst()
                    .ifPresent(settings -> body.put("settings", settings(settings)));
                body.put("recurring", schedules(name));
                return body;
            }));
            case "limits" -> found(engine.limits().find(name).map(TaskEndpoint::limit));
            case "tasks" -> found(number(name).flatMap(this::task).map(TaskEndpoint::task));
            default -> notFound();
        };
    }

    @ReadOperation
    public WebEndpointResponse<Object> readPart(@Selector String kind, @Selector String name, @Selector String part,
                                                @Nullable Long after, @Nullable Integer limit) throws Exception {
        if (!kind.equals("topics") || !Set.of("dead-letters", "failing").contains(part)) {
            return notFound();
        }
        if (status(name).isEmpty()) {
            return notFound();
        }
        if (part.equals("failing")) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("failing", engine.failing(name, limit == null ? 50 : Math.clamp(limit, 1, 1_000)).stream().map(group -> {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("error", group.error());
                entry.put("tasks", group.tasks());
                entry.put("oldestFailedAt", text(group.oldestFailedAt()));
                entry.put("example", group.example());
                entry.put("exampleMessage", group.exampleMessage());
                return entry;
            }).toList());
            return new WebEndpointResponse<>(body);
        }
        int size = limit == null ? DEAD_LETTER_PAGE : Math.clamp(limit, 1, 1_000);
        List<TaskStore.TaskView> page = engine.deadLetters(name, after == null ? 0 : after, size);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("deadLetters", page.stream().map(TaskEndpoint::task).toList());
        // The next page starts after the last id of a full one.
        body.put("next", page.size() == size ? page.getLast().id() : null);
        return new WebEndpointResponse<>(body);
    }

    /** Topic actions: pause, resume, create, revive, discard, purge. */
    @WriteOperation
    public WebEndpointResponse<Object> act(@Selector String kind, @Selector String name, @Selector String action,
                                           @Nullable String ids, @Nullable String outcome, @Nullable String before) throws Exception {
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
            case "pause" -> engine.pause(name);
            case "resume" -> engine.resume(name);
            case "create" -> engine.createTopic(name);
            case "revive" -> body.put("revived", engine.revive(name, taskIds));
            case "discard" -> body.put("discarded", engine.discard(name, taskIds));
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
                body.put("purged", engine.purge(name, purged, until));
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

    /** Resets a rate limit to its configured values on every node. */
    @DeleteOperation
    public WebEndpointResponse<Object> reset(@Selector String kind, @Selector String name, @Nullable Long version, SecurityContext security)
        throws Exception {
        if (!kind.equals("limits") || engine.limits().find(name).isEmpty()) {
            return notFound();
        }
        try {
            return new WebEndpointResponse<>(limit(engine.limits().reset(name, who(security), version)));
        } catch (VersionConflictException e) {
            return new WebEndpointResponse<>(error(e.getMessage()), CONFLICT);
        }
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
        try {
            return engine.schedules().stream().filter(schedule -> schedule.topic().equals(topic)).map(schedule -> {
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
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Optional<TaskEngine.TopicStatus> status(String name) throws Exception {
        return engine.topicStatus().stream().filter(status -> status.name().equals(name)).findFirst();
    }

    private Optional<TaskStore.TaskView> task(long id) {
        try {
            return engine.find(id);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Map<String, Object> topic(TaskEngine.TopicStatus status) {
        Map<String, Object> topic = new LinkedHashMap<>();
        topic.put("name", status.name());
        topic.put("module", status.module());
        topic.put("paused", status.paused());
        topic.put("processing", status.processing());
        TaskStore.QueueCounts counts = status.counts();
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
        TaskStore.LimitOverride override = limit.override();
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

    private static Map<String, Object> rate(RateLimit limit) {
        if (limit == null) {
            return null;
        }
        Map<String, Object> rate = new LinkedHashMap<>();
        rate.put("rate", Rate.perSecond(limit.perSecond()).toString());
        rate.put("burst", limit.burst());
        return rate;
    }

    private static Map<String, Object> task(TaskStore.TaskView view) {
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
        task.put("payload", view.payload());
        task.put("output", view.output());
        task.put("error", view.error());
        task.put("failures", view.failures());
        task.put("firstFailedAt", text(view.firstFailedAt()));
        task.put("createdAt", text(view.createdAt()));
        task.put("completedAt", text(view.completedAt()));
        return task;
    }

    private static String text(Instant instant) {
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

    private static WebEndpointResponse<Object> badRequest(String message) {
        return new WebEndpointResponse<>(error(message), WebEndpointResponse.STATUS_BAD_REQUEST);
    }

    private static Map<String, Object> error(String message) {
        return Map.of("error", message == null ? "invalid request" : message);
    }
}
