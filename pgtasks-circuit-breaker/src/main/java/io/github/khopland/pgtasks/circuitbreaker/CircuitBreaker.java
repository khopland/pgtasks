package io.github.khopland.pgtasks.circuitbreaker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.extension.BatchListener;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.engine.extension.SettingKey;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stops claiming from a topic when most of its recent tasks fail: a downstream system is probably
 * down, and claiming more only fails more. While open, the node sends one probe every
 * {@link #PROBE_EVERY}, a claim of a single task whose failure doesn't use up an attempt, and the
 * probe's success closes it. Per node: each node opens and closes its own.
 *
 * <p>It opens when {@link #OPENS_AT} of the last {@link #WINDOW} results on this node failed
 * (80 % of 50 by default), from at least {@link #MIN_TASKS} different tasks (5). A single task
 * that keeps failing doesn't open it, however often it retries, so a poison task still reaches
 * the dead letters while the rest of the topic runs. Failures that are the task's own fault don't
 * count: those the retry policy dead-letters at once ({@code Retry.deadLetterAt}), such as bad
 * input, an undecodable payload, and a task out of attempts.
 *
 * <p>Register one instance with {@code TaskEngine.builder().extension(new CircuitBreaker())} or,
 * with Spring, as an {@link Extension} bean. It then guards every topic of the engine;
 * {@link #ENABLED} turns it off for a topic, or, in the defaults, for all of them. An instance
 * installed in several engines keeps the breakers of each apart. The starter's endpoint and
 * dashboard show the open breakers, under {@code breaker}, and close one on an operator's word; the
 * health shows each open topic as held back by {@code CircuitBreaker}.
 */
public final class CircuitBreaker implements Extension {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreaker.class);

    /** Whether the breaker guards the topic: true by default. Overrides take true/false or on/off. */
    public static final SettingKey<Boolean> ENABLED =
        SettingKey.of("breaker.enabled", Boolean.class, true, CircuitBreaker::onOff);

    /** The share of the last {@link #WINDOW} results that must be failures: above 0, at most 1; 0.8 by default. */
    public static final SettingKey<Double> OPENS_AT =
        SettingKey.of("breaker.opens-at", Double.class, 0.8, Double::valueOf).checking(share -> {
            if (!(share > 0 && share <= 1)) {
                throw new IllegalArgumentException("must be above 0 and at most 1, not " + share);
            }
        });

    /** How many recent results on this node count: 50 by default. */
    public static final SettingKey<Integer> WINDOW =
        SettingKey.of("breaker.window", Integer.class, 50, Integer::valueOf).checking(CircuitBreaker::positive);

    /** How many different tasks must have failed among them: 5 by default. */
    public static final SettingKey<Integer> MIN_TASKS =
        SettingKey.of("breaker.min-tasks", Integer.class, 5, Integer::valueOf).checking(CircuitBreaker::positive);

    /** How long to wait between probes while open, by the store's clock: 30 s by default. Overrides take {@code 30s} or ISO-8601. */
    public static final SettingKey<Duration> PROBE_EVERY =
        SettingKey.of("breaker.probe-every", Duration.class, Duration.ofSeconds(30), CircuitBreaker::interval).checking(every -> {
            if (!every.isPositive()) {
                throw new IllegalArgumentException("must be positive, not " + every);
            }
        });

    /** The breakers of each engine this is installed in. */
    private final List<Breakers> installed = new CopyOnWriteArrayList<>();

    /** Creates the extension; one instance serves every topic of the engines it is installed in. */
    public CircuitBreaker() {
    }

    @Override
    public List<SettingKey<?>> settings() {
        return List.of(ENABLED, OPENS_AT, WINDOW, MIN_TASKS, PROBE_EVERY);
    }

    @Override
    public void install(ExtensionContext context) {
        Breakers breakers = new Breakers(context);
        installed.add(breakers);
        context.claimGate(breakers);
        context.batchListener(breakers::record);
        // A restarted engine starts with every breaker closed, as it would on a new node.
        context.onStop(breakers::clear);
        context.operatorView(breakers.view());
    }

    /** Whether the topic's breaker is open on this node, in any engine this is installed in. */
    public boolean isOpen(TopicRef<?> topic) {
        return installed.stream().anyMatch(breakers -> breakers.isOpen(topic.name()));
    }

    /** The topics whose breaker is open on this node, by name, in name order. */
    public List<String> openTopics() {
        return installed.stream().flatMap(Breakers::openTopics).distinct().sorted().toList();
    }

    /**
     * Closes the topic's breaker on this node, as if a probe had succeeded: it claims as usual at once.
     *
     * @return false if it wasn't open
     */
    public boolean close(TopicRef<?> topic) {
        boolean closed = false;
        for (Breakers breakers : installed) {
            closed |= breakers.close(topic.name());
        }
        return closed;
    }

    /** The breakers of one engine: a claim gate, told each batch's results. */
    private static final class Breakers implements ClaimGate {

        /** A topic the breaker doesn't guard. */
        private static final Optional<TopicBreaker> OFF = Optional.empty();

        private final ExtensionContext context;
        private final Map<String, Optional<TopicBreaker>> topics = new ConcurrentHashMap<>();

        Breakers(ExtensionContext context) {
            this.context = context;
        }

        @Override
        public Permit permit(TopicRef<?> topic, int wanted) {
            Optional<TopicBreaker> breaker = breaker(topic);
            return breaker.isEmpty() ? Permit.ANY : breaker.get().permit();
        }

        @Override
        public void claimed(TopicRef<?> topic, List<Long> taskIds) {
            breaker(topic).ifPresent(breaker -> breaker.claimed(taskIds));
        }

        void record(TopicRef<?> topic, List<BatchListener.Completed> tasks) {
            Optional<TopicBreaker> found = breaker(topic);
            if (found.isEmpty()) {
                return;
            }
            TopicBreaker breaker = found.get();
            switch (breaker.record(tasks)) {
                case OPENED -> LOGGER.warn("Most recent tasks in {} fail; this node stops claiming from it and probes with one task every {}. "
                    + "Its breaker {}.", topic.name(), breaker.probeEvery(), breaker.describe());
                case CLOSED -> {
                    LOGGER.info("A probe in {} succeeded; this node claims from it as usual again", topic.name());
                    context.wake(topic);
                }
                case NONE -> { }
            }
        }

        boolean isOpen(String topic) {
            Optional<TopicBreaker> breaker = topics.getOrDefault(topic, OFF);
            return breaker.isPresent() && breaker.get().isOpen();
        }

        Stream<String> openTopics() {
            return topics.entrySet().stream().filter(entry -> entry.getValue().isPresent() && entry.getValue().get().isOpen()).map(Map.Entry::getKey);
        }

        boolean close(String topic) {
            Optional<TopicBreaker> breaker = topics.getOrDefault(topic, OFF);
            if (breaker.isEmpty() || !breaker.get().close()) {
                return false;
            }
            LOGGER.info("The breaker of {} was closed by hand; this node claims from it as usual again", topic);
            context.wake(topic);
            return true;
        }

        void clear() {
            topics.clear();
        }

        /** The topic's breaker, made from its settings the first time it is needed; empty when off. */
        private Optional<TopicBreaker> breaker(TopicRef<?> topic) {
            Optional<TopicBreaker> breaker = topics.get(topic.name());
            return breaker != null ? breaker : topics.computeIfAbsent(topic.name(), name -> create(topic));
        }

        private Optional<TopicBreaker> create(TopicRef<?> topic) {
            if (!context.topicSetting(topic, ENABLED).orElse(false)) {
                return OFF;
            }
            // The settings' checks have passed for every value, set in code or overridden.
            return Optional.of(new TopicBreaker(
                context.topicSetting(topic, OPENS_AT).orElseThrow(),
                context.topicSetting(topic, WINDOW).orElseThrow(),
                context.topicSetting(topic, MIN_TASKS).orElseThrow(),
                context.topicSetting(topic, PROBE_EVERY).orElseThrow(),
                context.clock()));
        }

        /** This node's open breakers, with "Close" on each. */
        OperatorView view() {
            return new OperatorView() {
                @Override
                public String name() {
                    return "breaker";
                }

                @Override
                public String title() {
                    return "Open circuit breakers";
                }

                @Override
                public String note() {
                    return "This node's breakers that are open now: each claims one probe task per interval, and closes when a probe succeeds. "
                        + "Every node opens and closes its own.";
                }

                @Override
                public List<Column> columns() {
                    return List.of(Column.of("topic", "Topic", Column.Kind.TOPIC), Column.of("opened", "Opened", Column.Kind.TIME),
                        Column.of("nextProbe", "Next probe", Column.Kind.TIME), Column.text("failed", "Failed"));
                }

                @Override
                public List<Row> rows() {
                    List<Row> rows = new ArrayList<>();
                    new TreeMap<>(topics).forEach((topic, breaker) -> {
                        TopicBreaker.Open open = breaker.map(TopicBreaker::open).orElse(null);
                        if (open != null) {
                            Map<String, Object> values = new LinkedHashMap<>();
                            values.put("topic", topic);
                            values.put("opened", open.openedAt());
                            values.put("nextProbe", open.nextProbe());
                            values.put("failed", open.failures() + " of the last " + open.window());
                            rows.add(new Row(topic, values, Set.of("close")));
                        }
                    });
                    return rows;
                }

                @Override
                public @Nullable TopicState topic(String topic) {
                    return isOpen(topic) ? TopicState.of("breaker open", "close") : null;
                }

                @Override
                public List<Action> actions() {
                    return List.of(Action.of("close", "Close breaker",
                        "This node claims from the topic as usual again, as if a probe had succeeded.", topic -> close(topic)));
                }
            };
        }
    }

    private static boolean onOff(String text) {
        return switch (text.trim().toLowerCase(Locale.ROOT)) {
            case "true", "on" -> true;
            case "false", "off" -> false;
            default -> throw new IllegalArgumentException("breaker.enabled takes true, false, on or off, not " + text);
        };
    }

    private static void positive(int value) {
        if (value < 1) {
            throw new IllegalArgumentException("must be at least 1, not " + value);
        }
    }

    private static final Pattern SHORT = Pattern.compile("(\\d+)\\s*(ms|s|m|h)");

    private static Duration interval(String text) {
        Matcher matcher = SHORT.matcher(text.trim().toLowerCase(Locale.ROOT));
        if (!matcher.matches()) {
            return Duration.parse(text.trim());
        }
        long amount = Long.parseLong(matcher.group(1));
        return switch (matcher.group(2)) {
            case "ms" -> Duration.ofMillis(amount);
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            default -> Duration.ofHours(amount);
        };
    }

    @Override
    public String toString() {
        return "CircuitBreaker" + openTopics();
    }
}
