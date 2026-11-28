package io.github.khopland.pgtasks.recurring;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.github.khopland.pgtasks.TaskWriter;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.HousekeepingJob;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.engine.extension.SettingKey;
import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pushes tasks on cron schedules, once per run across the cluster.
 *
 * <pre>
 * TaskEngine.builder()
 *     .extension(Recurring.of(
 *         Schedule.of(REPORTS, "nightly", Cron.parse("0 2 * * *", ZoneId.of("Europe/Oslo")), new ReportRequest("daily"))))
 *     ...
 * </pre>
 *
 * With Spring, declare a {@code Recurring} bean instead. Each node fires the schedules it
 * declares: a due run is locked in the store, and the push and the move to the next run commit
 * together, so a run fires once however many nodes declare it, and a node that dies halfway
 * leaves the run to another. The housekeeping leader isn't needed, so a schedule fires even
 * when the leader is a node of another application that shares the schema.
 *
 * <ul>
 *   <li>A run is pushed up to {@link #INTERVAL} after it is due, one second unless set.</li>
 *   <li>Runs missed while no node that declares the schedule was up are made up once, not once
 *       per missed run; the next run counts from then.</li>
 *   <li>A run whose push fails, say because its topic doesn't exist yet, is logged and tried again
 *       a minute later, or at the next run if that comes first. The other schedules still fire.</li>
 *   <li>Each push {@link io.github.khopland.pgtasks.Insertion#SUPERSEDE supersedes} a run that is
 *       still waiting, unless the schedule says otherwise, so runs don't pile up.</li>
 * </ul>
 *
 * <p>A schedule is kept by its topic and name. A node that starts with another cron or zone for
 * it moves its next run to what the new one says; one that keeps them leaves the next run alone.
 * While old and new nodes both run during a deploy, each moves the schedule on by its own cron,
 * so change a schedule's cron in a release that every node runs before relying on it. A schedule
 * that no running node declares stops firing, and is deleted after 30 days.
 *
 * <p>Its {@code schedules} view shows every stored schedule in the starter's endpoint and
 * dashboard, with Run now on those this node declares.
 */
public final class Recurring implements Extension {

    /** How often a node looks for due runs unless set. */
    public static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(1);

    /**
     * How often a node looks for due runs: a run is pushed up to this long after it is due. An
     * engine-wide setting: {@code TaskEngine.builder().set(Recurring.INTERVAL, ...)}, or the
     * override {@code extensions.recurring.interval} (in Spring {@code pgtasks.extensions.recurring.interval}),
     * which takes {@code 5s} or ISO-8601.
     */
    public static final SettingKey<Duration> INTERVAL =
        SettingKey.of("recurring.interval", Duration.class, DEFAULT_INTERVAL, Recurring::duration).checking(interval -> {
            if (!interval.isPositive()) {
                throw new IllegalArgumentException("must be positive, not " + interval);
            }
        });

    /** The job that fires the due runs, on every node. */
    static final String JOB = "recurring tasks";
    /** How soon a run whose push failed is tried again, unless the next run comes first. */
    static final Duration RETRY_AFTER = Duration.ofMinutes(1);
    /** How often a node marks its schedules as seen, and deletes those no node has seen for {@link #FORGET_AFTER}. */
    static final Duration TIDY_EVERY = Duration.ofHours(1);
    /** A schedule no running node has declared for this long is deleted. */
    static final Duration FORGET_AFTER = Duration.ofDays(30);
    private static final int BATCH = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(Recurring.class);

    private final Map<ScheduleStore.Key, Schedule<?>> schedules;
    /** One per engine this is installed in. */
    private final List<Runner> runners = new CopyOnWriteArrayList<>();

    private Recurring(Map<ScheduleStore.Key, Schedule<?>> schedules) {
        this.schedules = schedules;
    }

    public static Recurring of(Schedule<?>... schedules) {
        return of(List.of(schedules));
    }

    /**
     * @throws IllegalArgumentException if two schedules of a topic have the same name
     */
    public static Recurring of(Collection<? extends Schedule<?>> schedules) {
        Map<ScheduleStore.Key, Schedule<?>> byKey = new LinkedHashMap<>();
        for (Schedule<?> schedule : schedules) {
            Objects.requireNonNull(schedule, "schedule");
            if (byKey.putIfAbsent(schedule.key(), schedule) != null) {
                throw new IllegalArgumentException("Two schedules of topic " + schedule.topic().name() + " are named '" + schedule.name() + "'");
            }
        }
        return new Recurring(Collections.unmodifiableMap(byKey));
    }

    public List<Schedule<?>> schedules() {
        return List.copyOf(schedules.values());
    }

    @Override
    public List<SettingKey<?>> engineSettings() {
        return List.of(INTERVAL);
    }

    /**
     * Readies the schedules' storage on every start, before the topics and jobs run. With no
     * schedules of its own, a node only shows the others' to operators.
     */
    @Override
    public void install(ExtensionContext context) {
        Runner runner = new Runner(context, context.capability(ScheduleStore.class));
        runners.add(runner);
        context.onStart(runner::start);
        if (!schedules.isEmpty()) {
            runner.job = context.job(HousekeepingJob.everyNode(JOB, context.setting(INTERVAL), runner::run));
        }
        context.operatorView(runner.view());
    }

    /**
     * Every stored schedule, by topic and name, with when it runs next and last ran: this node's,
     * and those other nodes declare. For operators: the starter's endpoint and dashboard show it.
     *
     * @throws IllegalStateException before this is installed in an engine
     */
    public List<Status> status() {
        return installed().status();
    }

    /**
     * Makes a schedule's next run due now and runs the job at once, so it is pushed within moments,
     * as if its time had come; the run after it is the schedule's next time from then. Only a
     * schedule this node declares.
     *
     * @return false if this node declares no such schedule, or it isn't stored yet
     * @throws IllegalStateException before this is installed in an engine
     */
    public boolean runNow(String topic, String name) {
        return installed().runNow(new ScheduleStore.Key(topic, name));
    }

    private Runner installed() {
        if (runners.isEmpty()) {
            throw new IllegalStateException("Recurring isn't installed in an engine yet");
        }
        return runners.getFirst();
    }

    @Override
    public String toString() {
        return "Recurring" + schedules.values();
    }

    /**
     * A stored schedule as an operator sees it.
     *
     * @param declared whether this node declares it; one no running node declares doesn't fire,
     *                 and is deleted 30 days after a node last declared it
     */
    public record Status(String topic, String name, String cron, String zone, Instant nextRunAt, @Nullable Instant lastRunAt, boolean declared) {
    }

    private static final Pattern SHORT = Pattern.compile("(\\d+)\\s*(ms|s|m|h)");

    private static Duration duration(String text) {
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

    /** One engine's firing of the schedules. */
    private final class Runner {

        private final ExtensionContext context;
        private final ScheduleStore store;
        private volatile Instant tidied = Instant.MIN;
        /** The job that fires the due schedules; none on a node without schedules of its own. Set while installing. */
        private ExtensionContext.@Nullable Job job;

        Runner(ExtensionContext context, ScheduleStore store) {
            this.context = context;
            this.store = store;
        }

        /** Before the jobs run, so the first run finds this node's schedules declared. */
        void start() {
            store.prepare();
            if (schedules.isEmpty()) {
                return;
            }
            Instant now = context.clock().instant();
            List<ScheduleStore.Declaration> declared = schedules.values().stream()
                .map(schedule -> new ScheduleStore.Declaration(schedule.key(), schedule.cron().expression(), schedule.cron().zone().getId(),
                    schedule.cron().following(now)))
                .toList();
            context.transact("Storing the recurring tasks " + schedules.values(), transaction -> {
                store.declare(transaction, declared);
                return null;
            });
            tidied = now;
        }

        List<Status> status() {
            return context.transact("Reading the recurring tasks", store::list).stream()
                .map(row -> new Status(row.key().topic(), row.key().name(), row.cron(), row.zone(), row.nextRunAt(), row.lastRunAt(),
                    schedules.containsKey(row.key())))
                .toList();
        }

        boolean runNow(ScheduleStore.Key key) {
            if (!schedules.containsKey(key)) {
                return false;
            }
            boolean stored = context.transact("Running recurring task " + key.name() + " of " + key.topic() + " now", transaction -> {
                boolean found = store.list(transaction).stream().anyMatch(row -> row.key().equals(key));
                if (found) {
                    store.fired(transaction, key, null, context.clock().instant());
                }
                return found;
            });
            ExtensionContext.Job firing = job;
            if (stored && firing != null) {
                firing.runSoon();
            }
            return stored;
        }

        /** Serialized, so a test that moves the clock and the scheduled job don't fire at once. */
        synchronized void run() throws SQLException {
            Instant now = context.clock().instant();
            if (!now.isBefore(tidied.plus(TIDY_EVERY))) {
                tidy(now);
            }
            while (fireDue() == BATCH) {
                // A full batch: there may be more.
            }
        }

        /** Marks this node's schedules as seen, and deletes those no node has seen for a long time. */
        private void tidy(Instant now) {
            int forgotten = context.transact("Tidying the recurring tasks", transaction -> {
                store.seen(transaction, schedules.keySet());
                return store.forget(transaction, now.minus(FORGET_AFTER));
            });
            tidied = now;
            if (forgotten > 0) {
                LOGGER.info("Deleted {} recurring task(s) that no node has declared for {}", forgotten, FORGET_AFTER);
            }
        }

        /**
         * Pushes the due runs, each in the same transaction as moving its schedule on, so a run
         * fires once even if this node dies halfway. Each has a savepoint of its own, so one whose
         * push fails is undone, logged and tried again later, and the others still fire.
         *
         * @return how many were due
         */
        private int fireDue() {
            return context.transact("Firing the due recurring tasks", transaction -> {
                List<ScheduleStore.Row> due = store.due(transaction, schedules.keySet(), BATCH);
                if (due.isEmpty()) {
                    return 0;
                }
                Instant now = context.clock().instant();
                TaskWriter writer = context.tasks(transaction);
                for (ScheduleStore.Row row : due) {
                    Schedule<?> schedule = schedules.get(row.key());
                    if (schedule != null) {
                        fire(transaction, writer, schedule, row, now);
                    }
                }
                return due.size();
            });
        }

        private void fire(StoreTransaction transaction, TaskWriter writer, Schedule<?> schedule, ScheduleStore.Row row, Instant now)
            throws SQLException {
            // The store decides what is due by its own clock (the database server's), this node by its
            // sample of it. From whichever is later, so a node a little behind can't give back the run
            // it just fired and fire it again.
            Instant from = now.isAfter(row.nextRunAt()) ? now : row.nextRunAt();
            Instant next = schedule.cron().following(from);
            StoreTransaction.Savepoint savepoint = transaction.savepoint();
            try {
                schedule.pushTo(writer);
                store.fired(transaction, row.key(), now, next);
                savepoint.release();
            } catch (SQLException | RuntimeException e) {
                undo(savepoint, e);
                Instant retry = now.plus(RETRY_AFTER);
                Instant at = next.isBefore(retry) ? next : retry;
                LOGGER.warn("Recurring task {} could not be pushed; tried again at {}", schedule, at, e);
                store.fired(transaction, row.key(), null, at);
            }
        }

        /** Rolls back to the savepoint; if even that fails, the transaction can't go on, and the whole run is retried. */
        private static void undo(StoreTransaction.Savepoint savepoint, Exception cause) throws SQLException {
            try {
                savepoint.rollback();
            } catch (SQLException | RuntimeException e) {
                e.addSuppressed(cause);
                throw e;
            }
        }

        /** Every stored schedule, with Run now on those this node declares. */
        OperatorView view() {
            return new OperatorView() {
                @Override
                public String name() {
                    return "schedules";
                }

                @Override
                public String title() {
                    return "Recurring tasks";
                }

                @Override
                public String note() {
                    return "Every node fires the schedules it declares, each run once. A schedule no running node declares doesn't fire, "
                        + "and is deleted 30 days after a node last declared it.";
                }

                @Override
                public List<Column> columns() {
                    return List.of(Column.of("topic", "Topic", Column.Kind.TOPIC), Column.of("name", "Name", Column.Kind.CODE),
                        Column.of("cron", "Cron", Column.Kind.CODE), Column.text("zone", "Zone"), Column.of("nextRunAt", "Next run", Column.Kind.TIME),
                        Column.of("lastRunAt", "Last run", Column.Kind.TIME), Column.of("declared", "Declared here", Column.Kind.FLAG));
                }

                @Override
                public List<Row> rows() {
                    return status().stream().map(status -> {
                        Map<String, Object> values = new LinkedHashMap<>();
                        values.put("topic", status.topic());
                        values.put("name", status.name());
                        values.put("cron", status.cron());
                        values.put("zone", status.zone());
                        values.put("nextRunAt", status.nextRunAt());
                        if (status.lastRunAt() != null) {
                            values.put("lastRunAt", status.lastRunAt());
                        }
                        values.put("declared", status.declared());
                        return new Row(key(status.topic(), status.name()), values, status.declared() ? Set.of("run") : Set.of());
                    }).toList();
                }

                @Override
                public List<Action> actions() {
                    return List.of(Action.of("run", "Run now", "Push a run now, as if its time had come. Its schedule goes on as before.", key -> {
                        int slash = key.indexOf('/');
                        if (slash < 1 || !runNow(new ScheduleStore.Key(key.substring(0, slash), key.substring(slash + 1)))) {
                            throw new IllegalStateException("No recurring task " + key + " that this node declares");
                        }
                    }));
                }
            };
        }
    }

    /** A schedule's key in the view: its topic, which has no slash, then its name. */
    static String key(String topic, String name) {
        return topic + "/" + name;
    }
}
