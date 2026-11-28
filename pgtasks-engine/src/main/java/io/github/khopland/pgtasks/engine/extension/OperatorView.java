package io.github.khopland.pgtasks.engine.extension;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

/**
 * What an extension shows operators and lets them do, in two parts, each optional: a table of
 * rows with actions on a row, such as recurring tasks with "Run now", and what it says about each
 * topic, a short status and actions on the topic, such as "paused" with "Resume". The starter's
 * endpoint serves it ({@code GET /actuator/tasks/extensions/{name}}, the overview, and each
 * topic), and its dashboard draws the table as a panel on the overview, the status next to each
 * topic, and the topic's actions on its page. Register one with {@link ExtensionContext#operatorView};
 * {@code TaskEngine.operatorViews()} lists them.
 *
 * <pre>{@code
 * context.operatorView(new OperatorView() {
 *     public String name() { return "pause"; }
 *     public String title() { return "Paused topics"; }
 *     public List<Column> columns() {
 *         return List.of(Column.of("topic", "Topic", Column.Kind.TOPIC), Column.of("since", "Paused", Column.Kind.TIME));
 *     }
 *     public List<Row> rows() {
 *         return paused().stream().map(p -> Row.of(p.topic(), Map.of("topic", p.topic(), "since", p.at()), "resume")).toList();
 *     }
 *     public TopicState topic(String topic) {
 *         return isPaused(topic) ? TopicState.of("paused", "resume") : TopicState.of(null, "pause");
 *     }
 *     public List<Action> actions() {
 *         return List.of(Action.of("pause", "Pause", "No node claims from it until it is resumed.", this::pause),
 *             Action.of("resume", "Resume", null, this::resume));
 *     }
 * });
 * }</pre>
 *
 * Every method is called on each request, on the request's thread, and {@link #topic} once per
 * topic shown: keep them quick. One that throws fails that request, and only that one.
 */
public interface OperatorView {

    /** Lowercase words with hyphens, such as {@code pause} or {@code run-now}. */
    Pattern NAME = Pattern.compile("[a-z][a-z0-9]*(-[a-z0-9]+)*");

    /** Its path in the endpoint, {@code extensions/<name>}: a lowercase word, unique per engine, such as {@code schedules}. */
    String name();

    /** The panel's heading, such as "Recurring tasks". */
    String title();

    /** A sentence under the panel saying what the rows mean, if any. */
    default @Nullable String note() {
        return null;
    }

    /** The table's columns, in order: which value of each row they show, and how. None, the default, for a view without a table. */
    default List<Column> columns() {
        return List.of();
    }

    /** The table's rows, as they are now; none by default. */
    default List<Row> rows() {
        return List.of();
    }

    /**
     * What operators can do: to a row, given the row's key, or to a topic, given its name. Each
     * row and {@linkplain #topic topic state} names the ones it offers now. None by default.
     */
    default List<Action> actions() {
        return List.of();
    }

    /**
     * What it says about a topic now, any topic in the store: a short status such as
     * {@code "paused"}, and the actions it offers on it, or null for nothing, the default.
     */
    default @Nullable TopicState topic(String topic) {
        return null;
    }

    /**
     * What a view says about one topic.
     *
     * @param status  shown next to the topic, such as "paused"; null for no status
     * @param actions the names of the {@linkplain #actions() actions} it offers on the topic now
     */
    record TopicState(@Nullable String status, Set<String> actions) {

        public TopicState {
            actions = Collections.unmodifiableSet(new LinkedHashSet<>(Objects.requireNonNull(actions, "actions")));
        }

        public static TopicState of(@Nullable String status, String... actions) {
            return new TopicState(status, Set.of(actions));
        }
    }

    /**
     * A column of the table.
     *
     * @param key   the value it shows from each row
     * @param label its heading
     */
    record Column(String key, String label, Kind kind) {

        /** How the dashboard draws a value; the endpoint returns it as text, a number or a boolean either way. */
        public enum Kind {
            /** Plain text. */
            TEXT,
            /** Text in a fixed-width font, such as a cron expression or an id. */
            CODE,
            /** An instant, drawn as "3 min ago" or "in 2 h". */
            TIME,
            /** A topic name, drawn as a link to the topic. */
            TOPIC,
            /** A boolean, drawn as yes or no. */
            FLAG
        }

        public Column {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(label, "label");
            Objects.requireNonNull(kind, "kind");
            if (key.isBlank()) {
                throw new IllegalArgumentException("A column needs the key of the value it shows");
            }
        }

        public static Column of(String key, String label, Kind kind) {
            return new Column(key, label, kind);
        }

        public static Column text(String key, String label) {
            return new Column(key, label, Kind.TEXT);
        }
    }

    /**
     * One row of the table.
     *
     * @param key     what an action on the row is given, such as a topic's or a schedule's name;
     *                unique within the view
     * @param values  by column key: text, a number, a boolean, an {@link java.time.Instant} or
     *                {@link java.time.Duration}; anything else is shown as text, and a column the
     *                row has no value for shows nothing
     * @param actions the names of the {@linkplain #actions() actions} this row offers now
     */
    record Row(String key, Map<String, Object> values, Set<String> actions) {

        public Row {
            Objects.requireNonNull(key, "key");
            values = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(values, "values")));
            actions = Collections.unmodifiableSet(new LinkedHashSet<>(Objects.requireNonNull(actions, "actions")));
        }

        public static Row of(String key, Map<String, ?> values, String... actions) {
            return new Row(key, Collections.unmodifiableMap(values), Set.of(actions));
        }
    }

    /**
     * Something operators can do to a row or a topic, such as "Run now" or "Pause".
     *
     * @param name    its path in the endpoint, {@code extensions/<view>/<name>}: lowercase words with hyphens
     * @param label   the button's text
     * @param confirm what the dashboard asks the operator to confirm before it acts, if it should ask
     * @param perform does it, given the row's key or the topic's name
     */
    record Action(String name, String label, @Nullable String confirm, Perform perform) {

        public Action {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(label, "label");
            Objects.requireNonNull(perform, "perform");
            if (!NAME.matcher(name).matches()) {
                throw new IllegalArgumentException("Invalid action name '" + name + "': lowercase words with hyphens, such as run-now");
            }
        }

        public static Action of(String name, String label, @Nullable String confirm, Perform perform) {
            return new Action(name, label, confirm, perform);
        }
    }

    /**
     * Performs an action on the row with this key, or the topic of this name; it throws to fail
     * the request with its message. The endpoint calls it only for a row or topic that offers it.
     */
    @FunctionalInterface
    interface Perform {
        void perform(String key) throws Exception;
    }
}
