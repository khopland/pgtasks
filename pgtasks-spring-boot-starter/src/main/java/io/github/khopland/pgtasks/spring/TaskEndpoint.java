package io.github.khopland.pgtasks.spring;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import io.github.khopland.pgtasks.Outcome;
import io.github.khopland.pgtasks.admin.DeadLetterQuery;
import io.github.khopland.pgtasks.admin.NodeView;
import io.github.khopland.pgtasks.admin.QueueCounts;
import io.github.khopland.pgtasks.admin.QueueState;
import io.github.khopland.pgtasks.admin.TaskQuery;
import io.github.khopland.pgtasks.admin.TaskView;
import io.github.khopland.pgtasks.admin.TopicStatus;
import io.github.khopland.pgtasks.engine.EffectiveSettings;
import io.github.khopland.pgtasks.engine.TaskEngine;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.SecurityContext;
import org.springframework.boot.actuate.endpoint.Show;
import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

/**
 * {@code /actuator/tasks}: the topics, their settings and dead letters, with the operator's
 * changes. Everything here is also library API, on {@link TaskEngine#admin()}.
 *
 * <pre>
 * GET    /actuator/tasks                                   topics by module, this node and every node
 * GET    /actuator/tasks/topics/{topic}                    status, counts, settings and where each came from
 * GET    /actuator/tasks/topics/{topic}/dead-letters?after=&amp;limit=
 * GET    /actuator/tasks/topics/{topic}/tasks?after=&amp;limit=&amp;identifier=&amp;reference=&amp;state=ready,retrying&amp;header=tenant=north
 *                                                        &amp;createdFrom=&amp;createdUntil=   unfinished tasks, lowest id first
 * POST   /actuator/tasks/topics/{topic}/revive | discard   {"ids": "1,2"}; or {"error": "java.net.ConnectException",
 *                                                        "failedFrom": "...", "failedUntil": "..."}; or {"all": "true"}
 * POST   /actuator/tasks/topics/{topic}/release            suspended tasks: {"ids": "1,2"}; or {"identifier": "order-7"}; or {"all": "true"}
 * POST   /actuator/tasks/topics/{topic}/run-now            delayed and retrying tasks: {"ids": "1,2"}; or {"identifier": "order-7"}; or {"all": "true"}
 * POST   /actuator/tasks/topics/{topic}/cancel             unfinished tasks but active ones: {"ids": "1,2"}; or {"identifier": "order-7"}; or {"all": "true"}
 * POST   /actuator/tasks/topics/{topic}/purge              {"outcome": "succeeded", "before": "2026-01-01T00:00:00Z"}
 * GET    /actuator/tasks/tasks/{id}                        a task
 * DELETE /actuator/tasks/topics/{topic}                     a topic no longer used, with its history
 * </pre>
 *
 * Request bodies are flat JSON objects of strings and numbers, as Actuator reads them,
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
    private static final int TASK_PAGE = 50;
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
            modules.computeIfAbsent(status.module() == null ? "(other nodes)" : status.module(), ignored -> new ArrayList<>())
                .add(status.name());
            return topic(status);
        }).toList();
        body.put("modules", modules);
        body.put("topics", topics);
        body.put("node", node());
        body.put("nodes", engine.admin().nodes().stream().map(TaskEndpoint::node).toList());
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
                return body;
            }));
            case "tasks" -> found(number(name).flatMap(this::task).map(view -> task(view, shown)));
            default -> notFound();
        };
    }

    /**
     * A topic's dead letters, or its unfinished tasks that match the parameters
     * ({@code state} a comma-separated list, {@code header} one {@code name=value}).
     */
    @ReadOperation
    public WebEndpointResponse<Object> readPart(@Selector String kind, @Selector String name, @Selector String part,
                                                @Nullable Long after, @Nullable Integer limit, @Nullable String identifier,
                                                @Nullable String reference, @Nullable String state, @Nullable String header,
                                                @Nullable String createdFrom, @Nullable String createdUntil,
                                                @Nullable SecurityContext security) throws Exception {
        boolean shown = show.isShown(security == null ? SecurityContext.NONE : security, roles);
        if (!kind.equals("topics") || !Set.of("dead-letters", "tasks").contains(part)) {
            return notFound();
        }
        if (status(name).isEmpty()) {
            return notFound();
        }
        if (part.equals("tasks")) {
            TaskQuery query;
            try {
                query = filter(TaskQuery.ALL, identifier, reference, state, header, createdFrom, createdUntil).after(after == null ? 0 : after);
            } catch (IllegalArgumentException | DateTimeParseException e) {
                return badRequest(e.getMessage());
            }
            int size = limit == null ? TASK_PAGE : Math.clamp(limit, 1, 1_000);
            List<TaskView> page = engine.admin().tasks(name, query, size);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("tasks", page.stream().map(view -> task(view, shown)).toList());
            body.put("next", page.size() == size ? page.getLast().id() : null);
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

    /** Topic actions: revive, discard, release, run-now, cancel, purge. */
    @WriteOperation
    public WebEndpointResponse<Object> act(@Selector String kind, @Selector String name, @Selector String action,
                                           @Nullable String ids, @Nullable String outcome, @Nullable String before,
                                           @Nullable String error, @Nullable String failedFrom, @Nullable String failedUntil,
                                           @Nullable String all, @Nullable String identifier) throws Exception {
        if (!kind.equals("topics")) {
            return notFound();
        }
        if (status(name).isEmpty()) {
            return notFound();
        }
        List<Long> taskIds;
        try {
            taskIds = ids == null || ids.isBlank() ? null
                : Arrays.stream(ids.split(",")).map(String::strip).map(Long::valueOf).toList();
        } catch (NumberFormatException e) {
            return badRequest("ids is a comma-separated list of task ids, such as \"12,13\"");
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", name);
        body.put("action", action);
        switch (action) {
            case "revive", "discard" -> {
                DeadLetterQuery query;
                try {
                    query = deadLetterQuery(taskIds, error, failedFrom, failedUntil, all);
                } catch (IllegalArgumentException | DateTimeParseException e) {
                    return badRequest(e.getMessage());
                }
                body.put(action.equals("revive") ? "revived" : "discarded",
                    action.equals("revive") ? engine.admin().revive(name, query) : engine.admin().discard(name, query));
            }
            case "release", "run-now", "cancel" -> {
                String only = blankToNull(identifier);
                if (taskIds == null && only == null && !"true".equalsIgnoreCase(all)) {
                    String which = switch (action) {
                        case "release" -> "suspended";
                        case "run-now" -> "delayed or retrying";
                        default -> "unfinished";
                    };
                    return badRequest("Name the " + which + " tasks: \"ids\", or \"identifier\", or \"all\": \"true\" for every one");
                }
                TaskQuery query = (taskIds == null ? TaskQuery.ALL : TaskQuery.ids(taskIds)).withIdentifier(only);
                switch (action) {
                    case "release" -> body.put("released", engine.admin().release(name, query));
                    case "run-now" -> body.put("madeReady", engine.admin().runNow(name, query));
                    default -> body.put("cancelled", engine.admin().cancel(name, query));
                }
            }
            case "purge" -> {
                if (outcome == null || before == null) {
                    return badRequest("purge needs an outcome (succeeded, filtered, failed, redundant, recreated or cancelled) and a time to purge before");
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

    /** Deletes a topic no longer used, with its history (409 while it has unfinished tasks). */
    @DeleteOperation
    public WebEndpointResponse<Object> delete(@Selector String kind, @Selector String name) throws Exception {
        if (!kind.equals("topics") || status(name).isEmpty()) {
            return notFound();
        }
        try {
            return new WebEndpointResponse<>(Map.of("topic", name, "deletedTasks", engine.admin().deleteTopic(name)));
        } catch (IllegalStateException e) {
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

    /** The query narrowed by the request's parameters; blank ones are left out. */
    private static TaskQuery filter(TaskQuery query, @Nullable String identifier, @Nullable String reference, @Nullable String state,
                                    @Nullable String header, @Nullable String createdFrom, @Nullable String createdUntil) {
        String from = blankToNull(createdFrom), until = blankToNull(createdUntil), states = blankToNull(state), pair = blankToNull(header);
        TaskQuery narrowed = query.withIdentifier(blankToNull(identifier)).withReference(blankToNull(reference))
            .createdBetween(from == null ? null : Instant.parse(from.strip()), until == null ? null : Instant.parse(until.strip()));
        if (states != null) {
            narrowed = narrowed.inStates(Arrays.stream(states.split(",")).map(String::strip).filter(each -> !each.isEmpty()).map(QueueState::of).toList());
        }
        if (pair != null) {
            int equals = pair.indexOf('=');
            if (equals < 1) {
                throw new IllegalArgumentException("header is name=value, such as \"tenant=north\"");
            }
            narrowed = narrowed.withHeader(pair.substring(0, equals).strip(), pair.substring(equals + 1));
        }
        return narrowed;
    }

    private static @Nullable String blankToNull(@Nullable String text) {
        return text == null || text.isBlank() ? null : text;
    }

    private Map<String, Object> node() throws Exception {
        var health = engine.health();
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("name", health.node());
        node.put("id", health.nodeId());
        node.put("running", health.running());
        node.put("leader", health.leader());
        // A node whose heartbeat is older than this has stopped, or lost the database.
        node.put("leaseTermSeconds", health.leaseTerm().toSeconds());
        node.put("processing", engine.processing().stream().sorted().toList());
        return node;
    }

    private static Map<String, Object> node(NodeView view) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", view.id());
        node.put("name", view.name());
        node.put("version", view.version());
        node.put("startedAt", text(view.startedAt()));
        node.put("heartbeatAt", text(view.heartbeatAt()));
        node.put("leases", view.leases());
        return node;
    }

    private Optional<TopicStatus> status(String name) {
        return engine.admin().topic(name);
    }

    private Optional<TaskView> task(long id) {
        return engine.admin().find(id);
    }

    private static Map<String, Object> topic(TopicStatus status) {
        Map<String, Object> topic = new LinkedHashMap<>();
        topic.put("name", status.name());
        topic.put("module", status.module());
        topic.put("processing", status.processing());
        QueueCounts counts = status.counts();
        if (counts != null) {
            Map<String, Object> queue = new LinkedHashMap<>();
            queue.put("scheduled", counts.scheduled());
            queue.put("retrying", counts.retrying());
            queue.put("ready", counts.ready());
            queue.put("active", counts.active());
            queue.put("suspended", counts.suspended());
            queue.put("deadLetters", counts.deadLetters());
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
        task.put("error", view.error() == null ? null : shown ? StoredError.parse(view.error()) : StoredError.typeOnly(view.error(), HIDDEN));
        task.put("failures", view.failures());
        task.put("firstFailedAt", text(view.firstFailedAt()));
        task.put("createdAt", text(view.createdAt()));
        task.put("completedAt", text(view.completedAt()));
        Map<String, String> headers = new TreeMap<>();
        view.headers().forEach((name, value) -> headers.put(name, shown ? value : HIDDEN));
        task.put("headers", headers);
        return task;
    }

    private static @Nullable String text(@Nullable Instant instant) {
        return instant == null ? null : instant.toString();
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
