package org.example.task2.engine;

import java.time.Duration;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.example.task2.BatchHandler;
import org.example.task2.BatchResource;
import org.example.task2.Ordering;
import org.example.task2.Outcome;
import org.example.task2.PayloadCodec;
import org.example.task2.TopicRef;
import org.example.task2.engine.EffectiveSettings.Setting;
import org.example.task2.engine.EffectiveSettings.Source;
import org.example.task2.spi.Notifier;
import org.example.task2.spi.StoreProvider;
import org.example.task2.spi.TaskStore;
import org.example.task2.spi.TransactionBinding;

/**
 * Builds an engine from configurers. Settings come in layers, each overriding the one before:
 * built-in defaults, global defaults, the module's defaults, the topic's options, and overrides
 * (per-environment values such as YAML). Everything is merged and validated in {@link #build()}
 * before anything starts.
 *
 * <pre>{@code
 * TaskEngine engine = TaskEngine.builder()
 *     .dataSource(dataSource)              // or .store(new InMemoryTaskStore()) for local runs and tests
 *     .configure(notifyTasks)
 *     .configure(assessmentTasks)
 *     .build();
 * }</pre>
 */
public final class TaskEngineBuilder {

    static final String NO_STORE = "No task store. Give the builder a DataSource for Postgres (with task2-postgres on the "
        + "classpath), or choose the in-memory store explicitly with .store(new InMemoryTaskStore()); in Spring, "
        + "task.store=memory. The in-memory store is never chosen automatically, because it loses every task on restart.";

    private record Named<T>(String name, T value) {
    }

    private TaskStore store;
    private DataSource dataSource;
    private String node = "node";
    private final Map<String, String> storeOptions = new HashMap<>();
    private final List<Named<TaskConfigurer>> configurers = new ArrayList<>();
    private final List<Named<TaskGlobals>> globals = new ArrayList<>();
    private Map<String, String> overrides = Map.of();
    private TransactionBinding transactions = TransactionBinding.none();
    private TaskObserver observer = TaskObserver.NONE;
    private boolean topicsCreatedOnPush;
    private boolean notifications = true;
    private Notifier notifier;
    private final Map<Class<?>, PayloadCodec<?>> codecs = new HashMap<>();

    TaskEngineBuilder() {
    }

    /** Tasks live in this database, through the store a provider on the classpath offers for it. */
    public TaskEngineBuilder dataSource(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
        return this;
    }

    /**
     * This node's name, shown in tasks.node and in logs, such as the application's name. A
     * {@link TaskGlobals} that sets {@link GlobalOptions#node} takes precedence.
     */
    public TaskEngineBuilder node(String name) {
        this.node = Objects.requireNonNull(name);
        return this;
    }

    /** An explicit store, such as the in-memory store. */
    public TaskEngineBuilder store(TaskStore store) {
        this.store = Objects.requireNonNull(store);
        return this;
    }

    /** migrate (the default), validate or none: how a Postgres store treats its schema at startup. */
    public TaskEngineBuilder schemaMode(String mode) {
        storeOptions.put("schema-mode", mode);
        return this;
    }

    public TaskEngineBuilder configure(TaskConfigurer configurer) {
        return configure(nameOf(configurer), configurer);
    }

    /** @param name shown in errors and in the effective settings, such as a Spring bean name */
    public TaskEngineBuilder configure(String name, TaskConfigurer configurer) {
        configurers.add(new Named<>(name, Objects.requireNonNull(configurer)));
        return this;
    }

    public TaskEngineBuilder globals(TaskGlobals globals) {
        return globals(nameOf(globals), globals);
    }

    public TaskEngineBuilder globals(String name, TaskGlobals globals) {
        this.globals.add(new Named<>(name, Objects.requireNonNull(globals)));
        return this;
    }

    /**
     * Per-environment overrides, as flat properties: {@code topics.<topic>.concurrency},
     * {@code topics.<topic>.batch-size}, {@code ordering}, {@code poll-interval},
     * {@code batch-deadline}, {@code retry.max-attempts} (a number or {@code forever}),
     * {@code retry.give-up-after} (such as 7d, or {@code never}), {@code retry.first-backoff},
     * {@code retry.max-backoff}, {@code retain.<outcome>};
     * {@code limits.<limit>.rate} (such as 400/s) and {@code limits.<limit>.burst}. An unknown
     * topic, limit or key fails the build.
     */
    public TaskEngineBuilder overrides(Map<String, String> overrides) {
        this.overrides = Map.copyOf(overrides);
        return this;
    }

    public TaskEngineBuilder transactions(TransactionBinding transactions) {
        this.transactions = Objects.requireNonNull(transactions);
        return this;
    }

    /**
     * For module tests: a push to a topic that doesn't exist creates it, instead of failing, so a
     * test that runs one module's topics can push to another module's and assert what was pushed.
     * Nothing processes such a topic. Never in production, where a misspelt topic would quietly
     * collect tasks; the Spring starter turns it on only inside Spring Modulith module tests.
     */
    public TaskEngineBuilder topicsCreatedOnPush(boolean create) {
        this.topicsCreatedOnPush = create;
        return this;
    }

    /** Sees pushes and batches, for metrics and tracing, such as task2-micrometer's. */
    public TaskEngineBuilder observer(TaskObserver observer) {
        this.observer = Objects.requireNonNull(observer);
        return this;
    }

    /** Wake-ups; by default the store provider's, such as LISTEN/NOTIFY. */
    public TaskEngineBuilder notifier(Notifier notifier) {
        this.notifier = notifier;
        return this;
    }

    /** Off: rely on polling alone. */
    public TaskEngineBuilder notifications(boolean enabled) {
        this.notifications = enabled;
        return this;
    }

    public <T> TaskEngineBuilder codec(Class<T> type, PayloadCodec<T> codec) {
        codecs.put(type, Objects.requireNonNull(codec));
        return this;
    }

    private static String nameOf(Object configurer) {
        String name = configurer.getClass().getSimpleName();
        int lambda = name.indexOf("$$Lambda");
        return lambda < 0 ? name : name.substring(0, lambda);
    }

    /** One topic, or topic pattern, as registered, before overrides. */
    private static final class TopicSpec<T> {
        final String name;
        final Class<T> type;
        final boolean pattern;
        final BatchHandler<T> handler;
        final String module;
        final Spec spec;

        TopicSpec(String name, Class<T> type, boolean pattern, BatchHandler<T> handler, String module, Spec spec) {
            this.name = name;
            this.type = type;
            this.pattern = pattern;
            this.handler = handler;
            this.module = module;
            this.spec = spec;
        }
    }

    /** Collects options into values with their source. */
    private static final class Spec implements TopicOptions {
        final Map<String, Object> values = new LinkedHashMap<>();
        final Map<String, Source> sources = new LinkedHashMap<>();
        final List<BatchResource<?>> resources = new ArrayList<>();
        Source layer;

        Spec(Source layer) {
            this.layer = layer;
        }

        Spec set(String key, Object value) {
            values.put(key, Objects.requireNonNull(value, key));
            sources.put(key, layer);
            return this;
        }

        Spec apply(Source layer, Consumer<TopicOptions> options) {
            Source previous = this.layer;
            this.layer = layer;
            try {
                options.accept(this);
            } finally {
                this.layer = previous;
            }
            return this;
        }

        @Override
        public TopicOptions batchSize(int tasks) {
            return set("batch-size", positive("batch-size", tasks));
        }

        @Override
        public TopicOptions concurrency(int batches) {
            return set("concurrency", positive("concurrency", batches));
        }

        @Override
        public TopicOptions ordering(Ordering ordering) {
            return set("ordering", ordering);
        }

        @Override
        public TopicOptions retry(Retry retry) {
            return set("retry", retry);
        }

        @Override
        public TopicOptions batchDeadline(Duration deadline) {
            return set("batch-deadline", deadline);
        }

        @Override
        public TopicOptions pollInterval(Duration interval) {
            return set("poll-interval", interval);
        }

        @Override
        public TopicOptions limit(String name) {
            return set("limit", name);
        }

        @Override
        public TopicOptions resources(BatchResource<?>... resources) {
            this.resources.addAll(List.of(resources));
            return this;
        }

        @Override
        public TopicOptions retain(Outcome outcome, Period period) {
            return set("retain." + outcome.label(), period);
        }

        @Override
        public TopicOptions retainForever(Outcome outcome) {
            return set("retain." + outcome.label(), "forever");
        }

        private static int positive(String key, int value) {
            if (value < 1) {
                throw new IllegalArgumentException(key + " must be at least 1, was " + value);
            }
            return value;
        }
    }

    private static final class LimitSpec implements GlobalOptions.LimitOptions {
        Rate rate;
        Integer burst;
        boolean approximate;
        final List<RateWindow> windows = new ArrayList<>();

        @Override
        public GlobalOptions.LimitOptions window(Cron from, Rate rate) {
            windows.add(new RateWindow(from, rate));
            return this;
        }

        @Override
        public GlobalOptions.LimitOptions rate(Rate rate) {
            this.rate = Objects.requireNonNull(rate);
            return this;
        }

        @Override
        public GlobalOptions.LimitOptions burst(int permits) {
            if (permits < 1) {
                throw new IllegalArgumentException("burst must be at least 1");
            }
            this.burst = permits;
            return this;
        }

        @Override
        public GlobalOptions.LimitOptions approximate() {
            this.approximate = true;
            return this;
        }
    }

    private final class Globals implements GlobalOptions, GlobalOptions.HousekeepingOptions {
        EngineSettings settings = EngineSettings.defaults(node);
        final Spec defaults = new Spec(Source.GLOBAL_DEFAULTS);
        final Map<String, LimitSpec> limits = new LinkedHashMap<>();

        @Override
        public GlobalOptions node(String name) {
            settings = new EngineSettings(name, settings.heartbeat(), settings.expiry(), settings.promoteInterval(), settings.maxInFlightBatches(),
                settings.notifier(), settings.transactions(), settings.codecs(), settings.snapshotInterval(), settings.purgeInterval(), settings.observer());
            return this;
        }

        @Override
        public GlobalOptions heartbeat(Duration interval, Duration expiry) {
            settings = settings.withHeartbeat(interval, expiry);
            return this;
        }

        @Override
        public GlobalOptions maxInFlightBatches(int batches) {
            settings = settings.withMaxInFlightBatches(batches);
            return this;
        }

        @Override
        public GlobalOptions defaults(Consumer<TopicOptions> defaults) {
            this.defaults.apply(Source.GLOBAL_DEFAULTS, defaults);
            return this;
        }

        @Override
        public GlobalOptions limit(String name, Consumer<LimitOptions> limit) {
            LimitSpec spec = new LimitSpec();
            limit.accept(spec);
            if (spec.rate == null) {
                throw new IllegalArgumentException("Limit " + name + " needs a rate");
            }
            if (limits.putIfAbsent(name, spec) != null) {
                throw new IllegalArgumentException("Limit " + name + " is defined twice");
            }
            return this;
        }

        @Override
        public GlobalOptions housekeeping(Consumer<HousekeepingOptions> housekeeping) {
            housekeeping.accept(this);
            return this;
        }

        @Override
        public HousekeepingOptions retain(Outcome outcome, Period period) {
            defaults.set("retain." + outcome.label(), period);
            return this;
        }

        @Override
        public HousekeepingOptions retainForever(Outcome outcome) {
            defaults.set("retain." + outcome.label(), "forever");
            return this;
        }

        @Override
        public HousekeepingOptions promoteEvery(Duration interval) {
            settings = settings.withPromoteInterval(interval);
            return this;
        }

        @Override
        public HousekeepingOptions snapshotCountsEvery(Duration interval) {
            settings = settings.withSnapshotInterval(interval);
            return this;
        }

        @Override
        public HousekeepingOptions purgeEvery(Duration interval) {
            settings = settings.withPurgeInterval(interval);
            return this;
        }
    }

    public TaskEngine build() {
        List<String> problems = new ArrayList<>();
        if (store == null && dataSource == null) {
            throw new IllegalStateException(NO_STORE);
        }
        if (store != null && dataSource != null) {
            problems.add("Give the builder either a DataSource or a store, not both");
        }
        if (globals.size() > 1) {
            problems.add("At most one TaskGlobals per engine, found " + globals.stream().map(Named::name).toList());
        }
        Globals global = new Globals();
        globals.forEach(each -> each.value().configure(global));

        Map<String, TopicSpec<?>> topics = new LinkedHashMap<>();
        Map<Class<?>, PayloadCodec<?>> allCodecs = new HashMap<>(codecs);
        List<EventRoute<?>> routes = new ArrayList<>();
        List<Recurring<?>> recurring = new ArrayList<>();
        for (Named<TaskConfigurer> configurer : configurers) {
            Spec moduleDefaults = new Spec(Source.MODULE_DEFAULTS);
            List<TopicSpec<?>> registered = new ArrayList<>();
            configurer.value().configure(new TaskRegistry() {
                @Override
                public TaskRegistry defaults(Consumer<TopicOptions> defaults) {
                    moduleDefaults.apply(Source.MODULE_DEFAULTS, defaults);
                    return this;
                }

                @Override
                public <T> TaskRegistry topic(TopicRef<T> topic, BatchHandler<T> handler, Consumer<TopicOptions> options) {
                    Spec spec = new Spec(Source.TOPIC);
                    options.accept(spec);
                    registered.add(new TopicSpec<>(topic.name(), topic.payloadType(), false, Objects.requireNonNull(handler, "handler"), configurer.name(), spec));
                    return this;
                }

                @Override
                public <T> TaskRegistry topicsMatching(String pattern, Class<T> type, BatchHandler<T> handler, Consumer<TopicOptions> options) {
                    Spec spec = new Spec(Source.TOPIC);
                    options.accept(spec);
                    registered.add(new TopicSpec<>(Objects.requireNonNull(pattern, "pattern"), Objects.requireNonNull(type, "type"), true,
                        Objects.requireNonNull(handler, "handler"), configurer.name(), spec));
                    return this;
                }

                @Override
                public <T> TaskRegistry codec(Class<T> type, PayloadCodec<T> codec) {
                    PayloadCodec<?> previous = allCodecs.putIfAbsent(type, codec);
                    if (previous != null && previous != codec) {
                        problems.add("Two codecs for " + type.getName() + ", one from " + configurer.name());
                    }
                    return this;
                }

                @Override
                public <E> TaskRegistry onEvent(Class<E> event, java.util.function.Function<? super E, org.example.task2.Push<?>> route) {
                    routes.add(new EventRoute<>(event, route, configurer.name()));
                    return this;
                }

                @Override
                public <T> TaskRegistry recurring(TopicRef<T> topic, String name, Cron cron, String identifier, T payload,
                                                  org.example.task2.Insertion insertion) {
                    Recurring<T> schedule = new Recurring<>(topic, name, cron, identifier, payload, insertion, configurer.name());
                    if (registered.stream().noneMatch(spec -> !spec.pattern && spec.name.equals(topic.name()))) {
                        problems.add("Recurring task " + name + " is for topic " + topic.name() + ", which " + configurer.name()
                            + " doesn't register before it; a module schedules only its own topics");
                    } else if (recurring.stream().anyMatch(other -> other.topic().name().equals(topic.name()) && other.name().equals(name))) {
                        problems.add("Two recurring tasks named " + name + " for topic " + topic.name());
                    }
                    recurring.add(schedule);
                    return this;
                }
            });
            for (TopicSpec<?> topic : registered) {
                TopicSpec<?> previous = topics.putIfAbsent(topic.name, topic);
                if (previous != null) {
                    problems.add("Topic " + topic.name + " is registered by both " + previous.module + " and " + topic.module);
                }
                // Module defaults sit below the topic's own options.
                moduleDefaults.values.forEach((key, value) -> {
                    if (!topic.spec.values.containsKey(key)) {
                        topic.spec.values.put(key, value);
                        topic.spec.sources.put(key, Source.MODULE_DEFAULTS);
                    }
                });
                topic.spec.resources.addAll(0, moduleDefaults.resources);
            }
        }

        Map<String, Map<String, String>> topicOverrides = new LinkedHashMap<>();
        Map<String, Map<String, String>> limitOverrides = new LinkedHashMap<>();
        splitOverrides(topics.keySet(), global.limits.keySet(), topicOverrides, limitOverrides, problems);
        limitOverrides.forEach((name, values) -> values.forEach((key, value) -> {
            try {
                switch (key) {
                    case "rate" -> global.limits.get(name).rate = Rate.parse(value);
                    case "burst" -> global.limits.get(name).burst(Integer.parseInt(value.trim()));
                    default -> problems.add("Unknown limit setting limits." + name + "." + key + "; known: rate, burst");
                }
            } catch (IllegalArgumentException e) {
                problems.add("limits." + name + "." + key + ": " + e.getMessage());
            }
        }));

        List<Topic<?>> resolved = new ArrayList<>();
        List<TopicTemplate<?>> templates = new ArrayList<>();
        List<EffectiveSettings> effective = new ArrayList<>();
        for (TopicSpec<?> topic : topics.values()) {
            Map<String, Object> values = new LinkedHashMap<>();
            Map<String, Source> sources = new LinkedHashMap<>();
            layer(values, sources, defaults(), Source.DEFAULT);
            layer(values, sources, global.defaults.values, Source.GLOBAL_DEFAULTS);
            topic.spec.values.forEach((key, value) -> {
                values.put(key, value);
                sources.put(key, topic.spec.sources.get(key));
            });
            topicOverrides.getOrDefault(topic.name, Map.of()).forEach((key, value) -> {
                try {
                    Object parsed = parseOverride(key, value, (Retry) values.get("retry"));
                    // Each retry.* key changes one part of the retry policy, which is one setting.
                    String setting = key.startsWith("retry.") ? "retry" : key;
                    values.put(setting, parsed);
                    sources.put(setting, Source.OVERRIDE);
                } catch (IllegalArgumentException | DateTimeParseException e) {
                    problems.add("topics." + topic.name + "." + key + ": " + e.getMessage());
                }
            });
            try {
                Object built = build(topic, values, global, allCodecs);
                if (built instanceof TopicTemplate<?> template) {
                    templates.add(template);
                } else {
                    resolved.add((Topic<?>) built);
                }
            } catch (IllegalArgumentException e) {
                problems.add("Topic " + topic.name + " (" + topic.module + "): " + e.getMessage());
            }
            Map<String, Setting> report = new TreeMap<>();
            values.forEach((key, value) -> report.put(key, new Setting(describe(value), sources.get(key))));
            effective.add(new EffectiveSettings(topic.name, topic.module, report));
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("The task setup is invalid:\n - " + String.join("\n - ", problems));
        }

        TaskStore chosen = store;
        Notifier wakeUps = notifier;
        if (chosen == null) {
            StoreProvider provider = ServiceLoader.load(StoreProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .sorted(Comparator.comparingInt(StoreProvider::order))
                .filter(candidate -> candidate.supports(dataSource))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No task store supports " + dataSource + "; add task2-postgres to the classpath"));
            chosen = provider.create(dataSource, storeOptions);
            if (wakeUps == null && notifications) {
                wakeUps = provider.notifier(dataSource);
            }
        } else if (wakeUps == null && notifications) {
            wakeUps = notifierOf(chosen);
        }
        EngineSettings settings = global.settings.withNotifier(notifications ? wakeUps : null).withTransactions(transactions).withCodecs(allCodecs).withObserver(observer);
        TaskEngine engine = new TaskEngine(chosen, settings, resolved, templates).withEffectiveSettings(effective).withEventRoutes(routes)
            .withRecurring(recurring);
        return topicsCreatedOnPush ? engine.withTopicsCreatedOnPush() : engine;
    }

    /** The in-memory store offers its own in-process wake-ups. */
    private static Notifier notifierOf(TaskStore store) {
        try {
            return (Notifier) store.getClass().getMethod("notifier").invoke(store);
        } catch (ReflectiveOperationException e) {
            return null;
        }
    }

    private static Map<String, Object> defaults() {
        TopicSettings defaults = TopicSettings.defaults();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("batch-size", defaults.batchSize());
        values.put("concurrency", defaults.concurrency());
        values.put("ordering", defaults.ordering());
        values.put("retry", defaults.retry());
        values.put("batch-deadline", defaults.batchDeadline());
        values.put("poll-interval", defaults.pollInterval());
        for (Outcome outcome : Outcome.values()) {
            Period period = defaults.retention().get(outcome);
            values.put("retain." + outcome.label(), period == null ? "forever" : period);
        }
        return values;
    }

    private static void layer(Map<String, Object> values, Map<String, Source> sources, Map<String, Object> layer, Source source) {
        layer.forEach((key, value) -> {
            values.put(key, value);
            sources.put(key, source);
        });
    }

    /** A {@link Topic}, or a {@link TopicTemplate} for a pattern. */
    @SuppressWarnings("unchecked")
    private static <T> Object build(TopicSpec<T> topic, Map<String, Object> values, Globals global, Map<Class<?>, PayloadCodec<?>> codecs) {
        PayloadCodec<T> codec = (PayloadCodec<T>) codecs.get(topic.type);
        if (codec == null && topic.type == String.class) {
            codec = (PayloadCodec<T>) PayloadCodec.string();
        }
        if (codec == null) {
            throw new IllegalArgumentException("no payload codec for " + topic.type.getName() + "; register one with codec(...)");
        }
        RateLimit rateLimit = null;
        if (values.containsKey("limit")) {
            String name = (String) values.get("limit");
            LimitSpec limit = global.limits.get(name);
            if (limit == null) {
                throw new IllegalArgumentException("unknown limit '" + name + "'; define it in TaskGlobals; known: " + global.limits.keySet());
            }
            int burst = limit.burst != null ? limit.burst : (int) Math.max(1, Math.ceil(limit.rate.perSecond()));
            rateLimit = new RateLimit(name, limit.rate.perSecond(), burst, limit.approximate, limit.windows);
        }
        Map<Outcome, Period> retention = new EnumMap<>(Outcome.class);
        for (Outcome outcome : Outcome.values()) {
            if (values.get("retain." + outcome.label()) instanceof Period period) {
                retention.put(outcome, period);
            }
        }
        TopicSettings settings = new TopicSettings(
            (Integer) values.get("batch-size"),
            (Integer) values.get("concurrency"),
            (Ordering) values.get("ordering"),
            (Retry) values.get("retry"),
            (Duration) values.get("batch-deadline"),
            (Duration) values.get("poll-interval"),
            rateLimit,
            retention
        );
        return topic.pattern
            ? new TopicTemplate<>(topic.name, topic.type, topic.handler, codec, settings, topic.spec.resources)
            : new Topic<>(TopicRef.of(topic.name, topic.type), topic.handler, codec, settings, topic.spec.resources);
    }

    private void splitOverrides(java.util.Set<String> topics, java.util.Set<String> limits,
                                Map<String, Map<String, String>> topicOverrides, Map<String, Map<String, String>> limitOverrides,
                                List<String> problems) {
        for (Map.Entry<String, String> override : new TreeMap<>(overrides).entrySet()) {
            String key = override.getKey();
            if (key.startsWith("topics.")) {
                String rest = key.substring("topics.".length());
                String topic = longestPrefix(topics, rest);
                if (topic == null) {
                    problems.add("Override " + key + " names an unknown topic; known topics: " + topics);
                } else {
                    topicOverrides.computeIfAbsent(topic, ignored -> new LinkedHashMap<>()).put(rest.substring(topic.length() + 1), override.getValue());
                }
            } else if (key.startsWith("limits.")) {
                String rest = key.substring("limits.".length());
                String limit = longestPrefix(limits, rest);
                if (limit == null) {
                    problems.add("Override " + key + " names an unknown limit; known limits: " + limits);
                } else {
                    limitOverrides.computeIfAbsent(limit, ignored -> new LinkedHashMap<>()).put(rest.substring(limit.length() + 1), override.getValue());
                }
            } else {
                problems.add("Unknown override " + key + "; overrides start with topics. or limits.");
            }
        }
    }

    private static String longestPrefix(java.util.Set<String> names, String key) {
        return names.stream().filter(name -> key.startsWith(name + ".")).max(Comparator.comparingInt(String::length)).orElse(null);
    }

    private static Object parseOverride(String key, String value, Retry retry) {
        return switch (key) {
            case "batch-size", "concurrency" -> {
                int number = Integer.parseInt(value.trim());
                if (number < 1) {
                    throw new IllegalArgumentException("must be at least 1");
                }
                yield number;
            }
            case "ordering" -> Ordering.valueOf(value.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
            case "batch-deadline", "poll-interval" -> duration(value);
            case "retry.max-attempts" -> value.trim().equalsIgnoreCase("forever") ? retry.forever() : retry.attempts(Integer.parseInt(value.trim()));
            case "retry.give-up-after" -> Set.of("never", "forever").contains(value.trim().toLowerCase(Locale.ROOT))
                ? retry.forever() : retry.giveUpAfter(duration(value));
            case "retry.first-backoff" -> retry.withBackoff(duration(value), retry.max().compareTo(duration(value)) < 0 ? duration(value) : retry.max());
            case "retry.max-backoff" -> retry.withBackoff(retry.initial().compareTo(duration(value)) > 0 ? duration(value) : retry.initial(), duration(value));
            default -> {
                if (key.startsWith("retain.") && outcome(key.substring("retain.".length())) != null) {
                    yield value.trim().equalsIgnoreCase("forever") ? "forever" : period(value);
                }
                throw new IllegalArgumentException("unknown setting; known: batch-size, concurrency, ordering, batch-deadline, "
                    + "poll-interval, retry.max-attempts, retry.give-up-after, retry.first-backoff, retry.max-backoff, retain.<outcome>");
            }
        };
    }

    private static Outcome outcome(String label) {
        for (Outcome outcome : Outcome.values()) {
            if (outcome.label().equals(label)) {
                return outcome;
            }
        }
        return null;
    }

    private static final Pattern SHORT = Pattern.compile("\\s*(\\d+)\\s*(ms|s|m|h|d)\\s*");

    static Duration duration(String text) {
        Matcher matcher = SHORT.matcher(text.toLowerCase(Locale.ROOT));
        if (matcher.matches()) {
            long amount = Long.parseLong(matcher.group(1));
            return switch (matcher.group(2)) {
                case "ms" -> Duration.ofMillis(amount);
                case "s" -> Duration.ofSeconds(amount);
                case "m" -> Duration.ofMinutes(amount);
                case "h" -> Duration.ofHours(amount);
                default -> Duration.ofDays(amount);
            };
        }
        return Duration.parse(text.trim());
    }

    static Period period(String text) {
        Matcher matcher = SHORT.matcher(text.toLowerCase(Locale.ROOT));
        if (matcher.matches() && matcher.group(2).equals("d")) {
            return Period.ofDays(Integer.parseInt(matcher.group(1)));
        }
        return Period.parse(text.trim());
    }

    private static String describe(Object value) {
        return switch (value) {
            case Retry retry -> retry.describe();
            default -> String.valueOf(value);
        };
    }
}
