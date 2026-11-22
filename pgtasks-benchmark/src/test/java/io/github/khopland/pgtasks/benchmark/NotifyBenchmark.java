package io.github.khopland.pgtasks.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.JdbcTransactions;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.postgres.TaskSchema;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * Does waking workers limit how fast the application can commit, and how fast can pgtasks go?
 * Postgres serializes the commits of all transactions that called pg_notify behind one
 * database-wide lock (taken in PreCommit_Notify, held until the commit is flushed). A push the
 * engine sees commit ({@code tasks.push} in a bound transaction) notifies after the commit, from a
 * short debounced transaction of the engine's own; a push on an explicit connection
 * ({@code tasks.on(connection)}) still notifies from the queue's insert trigger, inside the
 * application's transaction. This benchmark measures both with the real engine and schema, fsync
 * and synchronous commit on, and writes target/notify-results.md.
 *
 * <p>Four parts, chosen with {@code benchmark.notify} (comma-separated, or {@code all}):
 * <ul>
 *   <li>{@code contention}: N producers, each committing transactions that insert one row into an
 *       application table and push one task, while one engine node consumes. Commits/s, commit
 *       latency, how many commits shared a WAL flush, and what the producers' backends waited on,
 *       for each {@link Variant}.</li>
 *   <li>{@code ceiling}: producers flat out against 1 or 3 engine nodes (separate engines with
 *       separate pools in this JVM), pushing one task or 100 per transaction, then paced at 90 % of
 *       that: what completes per second, and which side is the limit.</li>
 *   <li>{@code search}: the same shapes, with the offered rate raised step by step until the
 *       backlog grows; reports the last rate sustained.</li>
 *   <li>{@code latency}: a steady push rate, and the time from a push's commit to its handler
 *       starting, pushed on the consuming node or on another one, with LISTEN on and off.</li>
 * </ul>
 *
 * <pre>
 * mvn -Pbenchmark -pl pgtasks-benchmark -am test -Dtest=NotifyBenchmark -Dsurefire.failIfNoSpecifiedTests=false -Dbenchmark.notify=all
 * </pre>
 * Other system properties: {@code benchmark.producers} (default 1,8,32,64,128),
 * {@code benchmark.variants} (contention, default after-commit,trigger,none),
 * {@code benchmark.ceiling-variants} (default after-commit,trigger), {@code benchmark.search-variants}
 * (default after-commit), {@code benchmark.batches} (batches per node in ceiling and search runs,
 * default 2,8), {@code benchmark.seconds} (measured seconds per contention, ceiling and search run,
 * default 15) after {@code benchmark.warmup} (default 3), {@code benchmark.repeats} (contention
 * rounds, default 1), {@code benchmark.consumer} (default true; false runs the producers alone),
 * {@code benchmark.history} (finished tasks in the topic before each run, default 200000),
 * {@code benchmark.nodes} (default 1,3), {@code benchmark.rates} (latency runs' tasks/s, default
 * 100,1000), {@code benchmark.latency-seconds} (default 30; poll-only runs four times as long),
 * {@code benchmark.postgres.config} (extra server settings, {@code k=v;k=v}, such as
 * {@code wal_sync_method=fsync_writethrough} for a real flush on macOS),
 * {@code benchmark.schema} (default the library's, {@code pgtasks}) and {@code benchmark.data-directory}.
 */
@EnabledIfSystemProperty(named = "benchmark.notify", matches = ".+")
class NotifyBenchmark {

    private static final TopicRef<String> TOPIC = TopicRef.of("bench", String.class);
    private static final String TEMPLATE = "notify_template";
    /** The schema the tasks live in, the library's default unless {@code benchmark.schema} says otherwise. */
    private static final String SCHEMA = TaskSchema.validName(System.getProperty("benchmark.schema", TaskSchema.DEFAULT_SCHEMA));
    private static final String PRODUCER = "pgtasks-bench-producer";
    private static final String INSERT_EVENT = "INSERT INTO app.event (body) VALUES (?)";

    private static final List<String> PARTS = list("benchmark.notify", "all");
    private static final List<Integer> PRODUCERS = integers("benchmark.producers", "1,8,32,64,128");
    private static final List<Variant> VARIANTS = variants("benchmark.variants", "after-commit,trigger,none");
    private static final List<Variant> CEILING_VARIANTS = variants("benchmark.ceiling-variants", "after-commit,trigger");
    private static final List<Variant> SEARCH_VARIANTS = variants("benchmark.search-variants", "after-commit");
    private static final List<Integer> BATCHES = integers("benchmark.batches", "2,8");
    private static final Duration WARMUP = Duration.ofSeconds(Long.getLong("benchmark.warmup", 3));
    private static final Duration MEASURE = Duration.ofSeconds(Long.getLong("benchmark.seconds", 15));
    private static final boolean CONSUMER = Boolean.parseBoolean(System.getProperty("benchmark.consumer", "true"));
    private static final List<Integer> NODES = integers("benchmark.nodes", "1,3");
    private static final List<Integer> RATES = integers("benchmark.rates", "100,1000");
    private static final int REPEATS = Integer.getInteger("benchmark.repeats", 1);
    private static final long HISTORY = Long.getLong("benchmark.history", 200_000);
    private static final Duration LATENCY = Duration.ofSeconds(Long.getLong("benchmark.latency-seconds", 30));
    /** The contention runs' consumer polls this often, so the baseline without wake-ups still drains. */
    private static final Duration CONTENTION_POLL = Duration.ofSeconds(1);

    /** How a push wakes workers. */
    enum Variant {
        /**
         * As shipped for a push the engine sees commit: {@code tasks.push} inside
         * {@link JdbcTransactions#inTransaction}. The insert trigger skips it; after the commit the
         * engine wakes its own workers and sends a debounced pg_notify from its own transaction.
         */
        AFTER_COMMIT("notify after commit (shipped)"),
        /**
         * {@code tasks.on(connection)}: the insert trigger notifies inside the application's
         * transaction. What every push did before the notify moved after the commit, and what
         * pushes on an explicit connection still do.
         */
        TRIGGER("notify at commit (trigger)"),
        /** {@code tasks.on(connection)} with the insert trigger dropped: nothing notifies. The baseline. */
        NONE("no notify (baseline)");

        final String label;

        Variant(String label) {
            this.label = label;
        }

        void apply(Connection connection) throws SQLException {
            if (this == NONE) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(sql("DROP TRIGGER queue_insert_notify ON ${schema}.queue"));
                }
            }
        }
    }

    record Latency(double p50, double p99, double p999, double max) {

        static Latency of(List<Double> milliseconds) {
            if (milliseconds.isEmpty()) {
                return new Latency(Double.NaN, Double.NaN, Double.NaN, Double.NaN);
            }
            List<Double> sorted = new ArrayList<>(milliseconds);
            Collections.sort(sorted);
            return new Latency(at(sorted, 0.5), at(sorted, 0.99), at(sorted, 0.999), sorted.getLast());
        }

        private static double at(List<Double> sorted, double quantile) {
            return sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * quantile)));
        }
    }

    /** What the producers of one run did in the measured window. */
    record Load(long commits, double seconds, Latency commit, Latency transaction, long walFlushes, long notifications, long completed,
                long backlogStart, long backlog, List<String> waits, List<String> consumerWaits) {

        double commitsPerSecond() {
            return commits / seconds;
        }

        double commitsPerFlush() {
            return walFlushes == 0 ? Double.NaN : commits / (double) walFlushes;
        }

        double pushedPerSecond(Shape shape) {
            return commits * shape.perTransaction() / seconds;
        }

        double completedPerSecond() {
            return completed / seconds;
        }

        /** Whether a run paced at {@code rate} sustained it: the producers kept up, and the backlog grew by less than {@code growth}. */
        boolean held(Shape shape, int rate, long growth) {
            return pushedPerSecond(shape) >= 0.97 * rate && backlog - backlogStart < growth;
        }
    }

    private final StringBuilder report = new StringBuilder();
    private EmbeddedPostgres postgres;
    private int runs;

    @Test
    void notifyAndThroughput() throws Exception {
        Path logs = Path.of("target", "postgres-logs").toAbsolutePath();
        Files.createDirectories(logs);
        Files.deleteIfExists(logs.resolve("notify.log"));
        EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder()
            // Durability on: what production pays for every commit.
            .setServerConfig("fsync", "on")
            .setServerConfig("synchronous_commit", "on")
            .setServerConfig("full_page_writes", "on")
            .setServerConfig("shared_buffers", System.getProperty("benchmark.shared-buffers", "1GB"))
            .setServerConfig("work_mem", "16MB")
            .setServerConfig("maintenance_work_mem", "512MB")
            .setServerConfig("max_wal_size", "8GB")
            .setServerConfig("checkpoint_timeout", "15min")
            .setServerConfig("max_connections", "400")
            .setServerConfig("logging_collector", "on")
            .setServerConfig("log_directory", logs.toString())
            .setServerConfig("log_filename", "notify.log")
            .setServerConfig("log_rotation_size", "0")
            .setServerConfig("log_checkpoints", "on");
        for (String setting : System.getProperty("benchmark.postgres.config", "").split(";")) {
            if (setting.isBlank()) {
                continue;
            }
            String[] pair = setting.split("=", 2);
            builder.setServerConfig(pair[0].strip(), pair[1].strip());
        }
        String directory = System.getProperty("benchmark.data-directory");
        if (directory != null) {
            builder.setDataDirectory(Path.of(directory)).setCleanDataDirectory(true);
        }
        try (EmbeddedPostgres started = builder.start()) {
            postgres = started;
            settings();
            template();
            boolean all = PARTS.contains("all");
            if (all || PARTS.contains("contention")) {
                contention();
            }
            if (all || PARTS.contains("ceiling")) {
                ceiling();
            }
            if (all || PARTS.contains("search")) {
                search();
            }
            if (all || PARTS.contains("latency")) {
                latency();
            }
        }
        Files.writeString(Path.of("target", "notify-results.md"), report);
        log("%n%s", report);
    }

    /** The settings the server actually ran with, for the write-up. */
    private void settings() throws SQLException {
        try (Connection connection = admin(); Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("""
                 SELECT name, setting || coalesce(' ' || unit, '') FROM pg_settings
                 WHERE name IN ('server_version', 'fsync', 'synchronous_commit', 'full_page_writes', 'wal_sync_method', 'commit_delay',
                                'shared_buffers', 'max_wal_size', 'checkpoint_timeout', 'max_connections', 'wal_level', 'wal_buffers')
                 ORDER BY name
                 """)) {
            report.append("Postgres settings:");
            while (resultSet.next()) {
                report.append(' ').append(resultSet.getString(1)).append('=').append(resultSet.getString(2)).append(';');
            }
            report.append(String.format(Locale.ROOT, "%nWarm-up %d s, measured %d s per run; %,d finished tasks in the topic.%n",
                WARMUP.toSeconds(), MEASURE.toSeconds(), HISTORY));
        }
    }

    /** The schema and an application table, once; every run gets a fresh copy. */
    private void template() throws SQLException {
        try (Connection connection = admin(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + TEMPLATE);
        }
        try (HikariDataSource pool = pool(TEMPLATE, "pgtasks-bench-setup", 2, true)) {
            TaskSchema.migrate(pool, SCHEMA);
            try (Connection connection = pool.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA app");
                statement.execute("""
                    CREATE TABLE app.event (id bigserial PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now(), body text NOT NULL)
                    """);
                // Finished tasks in the topic, analyzed, as in a database that has run for a while.
                statement.execute(sql("""
                    INSERT INTO ${schema}.task (topic_id, id, identifier, payload, created_at, outcome, attempts, completed_at)
                    SELECT ${schema}.create_topic('%s'), nextval('${schema}.task_id_seq'), 'h' || g, 'payload',
                           now() - make_interval(secs => %d - g), 'succeeded', 1, now() - make_interval(secs => %d - g)
                    FROM generate_series(1, %d) g
                    """).formatted(TOPIC.name(), HISTORY, HISTORY, HISTORY));
                statement.execute("VACUUM ANALYZE");
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Part 1: producer contention

    private void contention() throws Exception {
        report.append(String.format(Locale.ROOT, """

            ## Producer contention

            Each transaction inserts one row into app.event and pushes one task. Consumer: %s. \
            Commit is the time in commit(); transaction is begin to commit returned. Commits per flush is commits \
            divided by WAL flushes (pg_stat_wal.wal_sync, or wal_write with an open_*sync method). Waits: average \
            number of producer backends in each state, sampled every 5 ms (CPU means active and not waiting); \
            consumer waits likewise for the engine node's backends, by statement.

            | Variant | Producers | Commits/s | Commit p50 / p99 ms | Transaction p50 / p99 ms | Commits per flush | \
            Notifications/s | Completed/s | Backlog at end | Waits | Consumer waits |
            |---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
            """, CONSUMER ? "1 engine node, LISTEN on, default batches, poll every " + CONTENTION_POLL.toSeconds() + " s" : "none"));
        // Variants interleaved, so drift over the run (checkpoints, the machine warming) spreads over all of them.
        Map<String, List<Load>> loads = new LinkedHashMap<>();
        for (int repeat = 1; repeat <= REPEATS; repeat++) {
            for (int producers : PRODUCERS) {
                for (Variant variant : VARIANTS) {
                    Load load = contention(variant, producers);
                    loads.computeIfAbsent(variant.label + "|" + producers, ignored -> new ArrayList<>()).add(load);
                    String row = String.format(Locale.ROOT, "| %s | %d | %,.0f | %.2f / %.2f | %.2f / %.2f | %.1f | %,.0f | %,.0f | %,d | %s | %s |",
                        variant.label, producers, load.commitsPerSecond(), load.commit().p50(), load.commit().p99(),
                        load.transaction().p50(), load.transaction().p99(), load.commitsPerFlush(), load.notifications() / load.seconds(),
                        load.completed() / load.seconds(), load.backlog(), String.join("; ", load.waits()),
                        String.join("; ", load.consumerWaits()));
                    report.append(row).append('\n');
                    log("%s", row);
                }
            }
        }
        // Commits/s by producers and variant, the median of the rounds, and commit p99 likewise.
        report.append(String.format(Locale.ROOT, "%nMedian of %d round(s): commits/s (commit p99 ms)%n%n| Producers |", REPEATS));
        VARIANTS.forEach(variant -> report.append(' ').append(variant.label).append(" |"));
        report.append("\n|---:|").append("---:|".repeat(VARIANTS.size())).append('\n');
        for (int producers : PRODUCERS) {
            report.append("| ").append(producers).append(" |");
            for (Variant variant : VARIANTS) {
                List<Load> rounds = loads.get(variant.label + "|" + producers);
                report.append(String.format(Locale.ROOT, " %,.0f (%.1f) |", median(rounds.stream().map(Load::commitsPerSecond).toList()),
                    median(rounds.stream().map(load -> load.commit().p99()).toList())));
            }
            report.append('\n');
        }
    }

    private Load contention(Variant variant, int producers) throws Exception {
        String database = fresh(variant);
        try (Run run = new Run(database, variant, CONSUMER ? 1 : 0, 2, CONTENTION_POLL, true, false)) {
            return run.load(producers, 1, 0, WARMUP, MEASURE, true);
        } finally {
            drop(database);
        }
    }

    private static double median(List<Double> values) {
        List<Double> sorted = values.stream().sorted().toList();
        int middle = sorted.size() / 2;
        return sorted.size() % 2 == 1 ? sorted.get(middle) : (sorted.get(middle - 1) + sorted.get(middle)) / 2;
    }

    // ---------------------------------------------------------------------------------------------
    // Part 2: throughput ceiling

    /** Producers and tasks per transaction. */
    record Shape(int producers, int perTransaction) {

        static final List<Shape> ALL = List.of(new Shape(64, 1), new Shape(8, 100));

        String label() {
            return producers + " × " + perTransaction;
        }
    }

    private void ceiling() throws Exception {
        report.append("""

            ## Throughput ceiling

            First the producers push as fast as they can while engine nodes claim and complete (no-op handler, batches \
            of 100); then, on a fresh database, they push at 90 % of the lower of what was pushed and completed, to \
            see whether that rate is sustained: it held if the producers pushed at least 97 % of it and the backlog \
            grew by less than a second's worth. Nodes are separate engines in this JVM, each with its own pool, all on \
            this machine with Postgres; the producers push through node 1.

            | Variant | Nodes | Batches per node | Producers × tasks per transaction | Flat out: pushed/s | completed/s | \
            Paced at /s | pushed/s | completed/s | Backlog at start / end | Held | Commit p50 / p99 ms | Waits (paced) | \
            Consumer waits (paced) |
            |---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---|---|
            """);
        for (Shape shape : Shape.ALL) {
            for (int nodes : NODES) {
                for (int batches : BATCHES) {
                    for (Variant variant : CEILING_VARIANTS) {
                        long before = System.nanoTime();
                        Load flat = ceiling(variant, nodes, batches, shape, 0);
                        int target = (int) (0.9 * Math.min(flat.pushedPerSecond(shape), flat.completedPerSecond()));
                        Load paced = ceiling(variant, nodes, batches, shape, target);
                        String row = String.format(Locale.ROOT,
                            "| %s | %d | %d | %s | %,.0f | %,.0f | %,d | %,.0f | %,.0f | %,d / %,d | %s | %.2f / %.2f | %s | %s |",
                            variant.label, nodes, batches, shape.label(), flat.pushedPerSecond(shape), flat.completedPerSecond(),
                            target, paced.pushedPerSecond(shape), paced.completedPerSecond(), paced.backlogStart(), paced.backlog(),
                            paced.held(shape, target, target) ? "yes" : "no", paced.commit().p50(), paced.commit().p99(),
                            String.join("; ", paced.waits()), String.join("; ", paced.consumerWaits()));
                        report.append(row).append('\n');
                        log("%s (%.0f s)", row, (System.nanoTime() - before) / 1e9);
                    }
                }
            }
        }
    }

    private Load ceiling(Variant variant, int nodes, int batches, Shape shape, int rate) throws Exception {
        String database = fresh(variant);
        try (Run run = new Run(database, variant, nodes, batches, null, true, false)) {
            return run.load(shape.producers(), shape.perTransaction(), rate, WARMUP, MEASURE, true);
        } finally {
            drop(database);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Part 3: stepped ceiling search

    private void search() throws Exception {
        report.append("""

            ## Stepped ceiling search

            A flat-out run gives a first guess. Then each step pushes at a fixed rate on a fresh database: from 60 % of \
            the lower of the flat-out pushed and completed rates, up by half while it holds, then halving the gap \
            between the last rate held and the first that didn't, three times. A step held if the producers pushed at \
            least 97 % of the rate and the backlog grew by less than a quarter of a second's worth, or than the tasks \
            the nodes can hold claimed at once (nodes × batches × 100), whichever is more. The search stops early if the \
            producers can't push the next step's rate (the producers are then the limit, marked "producers"). \
            Steps: rate, then ✓ held or ✗ not, with the backlog's growth (and what was pushed, if short of the rate).

            | Variant | Nodes | Batches per node | Producers × tasks per transaction | Flat out: pushed/s | completed/s | \
            Sustained /s | First not sustained /s | Steps |
            |---|---:|---:|---:|---:|---:|---:|---:|---|
            """);
        for (Shape shape : Shape.ALL) {
            for (int nodes : NODES) {
                for (int batches : BATCHES) {
                    for (Variant variant : SEARCH_VARIANTS) {
                        long before = System.nanoTime();
                        Load flat = ceiling(variant, nodes, batches, shape, 0);
                        double pushable = flat.pushedPerSecond(shape);
                        List<String> steps = new ArrayList<>();
                        int held = 0, failed = 0;
                        int rate = (int) (0.6 * Math.min(pushable, flat.completedPerSecond()));
                        String limit = "";
                        while (true) {
                            Load step = ceiling(variant, nodes, batches, shape, rate);
                            boolean ok = step.held(shape, rate, Math.max(rate / 4, nodes * batches * 100L));
                            boolean pushedShort = step.pushedPerSecond(shape) < 0.97 * rate;
                            steps.add(String.format(Locale.ROOT, "%,d %s (%+,d%s)", rate, ok ? "✓" : "✗", step.backlog() - step.backlogStart(),
                                pushedShort ? String.format(Locale.ROOT, ", pushed %,.0f/s", step.pushedPerSecond(shape)) : ""));
                            log("  search %s, %d node(s) × %d, %s: %s", variant.label, nodes, batches, shape.label(), steps.getLast());
                            if (ok) {
                                held = rate;
                            } else {
                                failed = rate;
                            }
                            int next;
                            if (failed == 0) {
                                next = (int) (rate * 1.5);
                                if (next > pushable * 1.05) {
                                    limit = " (producers)";
                                    break;
                                }
                            } else if (held == 0) {
                                next = rate / 2;
                                if (next < 10) {
                                    break;
                                }
                            } else if (steps.size() < bisected(steps)) {
                                next = (held + failed) / 2;
                            } else {
                                break;
                            }
                            rate = round(next);
                        }
                        String row = String.format(Locale.ROOT, "| %s | %d | %d | %s | %,.0f | %,.0f | %,d%s | %s | %s |",
                            variant.label, nodes, batches, shape.label(), pushable, flat.completedPerSecond(), held, limit,
                            failed == 0 ? "–" : String.format(Locale.ROOT, "%,d", failed), String.join(", ", steps));
                        report.append(row).append('\n');
                        log("%s (%.0f s)", row, (System.nanoTime() - before) / 1e9);
                    }
                }
            }
        }
    }

    /** The number of steps after which to stop: three halvings after the first step that didn't hold. */
    private static int bisected(List<String> steps) {
        for (int index = 0; index < steps.size(); index++) {
            if (steps.get(index).contains("✗")) {
                return index + 1 + 3;
            }
        }
        return Integer.MAX_VALUE;
    }

    /** Two significant digits, so the steps read as round numbers. */
    private static int round(int rate) {
        int unit = (int) Math.pow(10, Math.max(0, (int) Math.log10(Math.max(rate, 1)) - 1));
        return Math.max(1, rate / unit * unit);
    }

    // ---------------------------------------------------------------------------------------------
    // Part 4: end-to-end latency

    private void latency() throws Exception {
        report.append("""

            ## End-to-end latency

            8 producers at a steady total rate, one task per transaction; one engine node consumes, with default \
            settings (2 batches of 100, 30 s poll). The producers push through that node (its workers are woken \
            directly after the commit) or through a second engine that only pushes (the consumer learns of the \
            task from the notification or its poll). Latency is from the push's commit returning to the handler \
            starting, in ms.

            | Wake-up | Pushed on | LISTEN | Rate/s | Seconds | Tasks | p50 | p99 | p99.9 | max | Over 1 s |
            |---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
            """);
        record Case(Variant variant, boolean listen, boolean separatePusher) {
        }
        List<Case> cases = List.of(
            new Case(Variant.AFTER_COMMIT, true, false),
            new Case(Variant.AFTER_COMMIT, true, true),
            new Case(Variant.AFTER_COMMIT, false, false),
            new Case(Variant.AFTER_COMMIT, false, true),
            new Case(Variant.TRIGGER, true, true));
        for (int rate : RATES) {
            for (Case each : cases) {
                // Without LISTEN, a push from another node waits for the poll: run longer for a fair spread.
                Duration length = each.listen() || !each.separatePusher() ? LATENCY : LATENCY.multipliedBy(4);
                String database = fresh(each.variant());
                try (Run run = new Run(database, each.variant(), 1, 2, null, each.listen(), each.separatePusher())) {
                    Load load = run.load(8, 1, rate, Duration.ZERO, length, false);
                    // Let the last ones be handled, up to one poll interval.
                    run.awaitHandled(Duration.ofSeconds(35));
                    List<Double> latencies = run.latencies();
                    Latency latency = Latency.of(latencies);
                    long slow = latencies.stream().filter(value -> value > 1_000).count();
                    String row = String.format(Locale.ROOT, "| %s | %s | %s | %,d | %d | %,d | %.1f | %.1f | %.1f | %,.0f | %,d |",
                        each.variant().label, each.separatePusher() ? "another node" : "the consuming node", each.listen() ? "on" : "off",
                        rate, length.toSeconds(), latencies.size(), latency.p50(), latency.p99(), latency.p999(), latency.max(), slow);
                    report.append(row).append('\n');
                    log("%s (pushed %,.0f/s)", row, load.commits() / load.seconds());
                }
                drop(database);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** One database with engine nodes consuming from it and producers pushing into it. */
    private final class Run implements AutoCloseable {

        private final String database;
        private final Variant variant;
        private final List<HikariDataSource> pools = new ArrayList<>();
        private final List<TaskEngine> engines = new ArrayList<>();
        private final HikariDataSource producers;
        private final JdbcTransactions transactions;
        private final AtomicLong completed = new AtomicLong();
        private final Map<String, Long> committedAt = new ConcurrentHashMap<>();
        private final Map<String, Long> startedAt = new ConcurrentHashMap<>();
        private final TaskEngine pusher;
        private long backlogAtStart;

        /**
         * {@code nodes} engines consume (0: one engine is prepared to push, never started); the
         * producers push through the first, or with {@code separatePusher} through one more engine
         * that is only prepared.
         */
        Run(String database, Variant variant, int nodes, int concurrency, Duration poll, boolean listen, boolean separatePusher)
            throws Exception {
            this.database = database;
            this.variant = variant;
            producers = pool(database, PRODUCER, 160, variant == Variant.AFTER_COMMIT);
            transactions = new JdbcTransactions(producers);
            for (int node = 1; node <= Math.max(nodes, 1); node++) {
                engines.add(engine(node, concurrency, poll, listen, nodes > 0));
            }
            if (separatePusher) {
                engines.add(engine(0, concurrency, poll, listen, false));
                pusher = engines.getLast();
            } else {
                pusher = engines.getFirst();
            }
        }

        private TaskEngine engine(int node, int concurrency, Duration poll, boolean listen, boolean start) {
            HikariDataSource pool = pool(database, "pgtasks-bench-node-" + node, 2 * concurrency + 6, true);
            pools.add(pool);
            TaskEngine engine = TaskEngine.builder()
                .dataSource(pool)
                .transactions(transactions)
                .node("bench-" + node)
                .schema(SCHEMA)
                .notifications(listen)
                .configure("bench", tasks -> tasks.eachTask(TOPIC, work -> {
                    startedAt.putIfAbsent(work.identifier(), System.nanoTime());
                    completed.incrementAndGet();
                }, topic -> {
                    topic.concurrency(concurrency);
                    if (poll != null) {
                        topic.pollInterval(poll);
                    }
                }))
                .build();
            return start ? engine.start() : engine.prepare();
        }

        /**
         * Runs {@code threads} producers, each committing transactions that insert one app row and push
         * {@code perTransaction} tasks, flat out or at {@code rate} tasks/s in all, and measures after the warm-up.
         * {@code sampleWaits} is off for latency runs, where the samplers' queries every 5 ms would add to what is measured.
         */
        Load load(int threads, int perTransaction, int rate, Duration warmup, Duration measure, boolean sampleWaits) throws Exception {
            AtomicBoolean measuring = new AtomicBoolean(warmup.isZero());
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger sequence = new AtomicInteger();
            List<List<Double>> commits = new ArrayList<>(), transactionTimes = new ArrayList<>();
            List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch done = new CountDownLatch(threads);
            List<Thread> running = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                List<Double> commitTimes = new ArrayList<>(), times = new ArrayList<>();
                commits.add(commitTimes);
                transactionTimes.add(times);
                long every = rate == 0 ? 0 : 1_000_000_000L * threads * perTransaction / rate;
                long offset = every == 0 ? 0 : every * thread / threads;
                running.add(Thread.ofPlatform().name("producer-" + thread).start(() -> {
                    try {
                        long next = System.nanoTime() + offset;
                        while (!stop.get()) {
                            if (every > 0) {
                                long wait = next - System.nanoTime();
                                if (wait > 0) {
                                    Thread.sleep(Duration.ofNanos(wait));
                                }
                                next += every;
                            }
                            List<Creation<String>> creations = new ArrayList<>(perTransaction);
                            for (int task = 0; task < perTransaction; task++) {
                                creations.add(Creation.of("t" + sequence.incrementAndGet(), "payload"));
                            }
                            long begin = System.nanoTime();
                            long committing = variant == Variant.AFTER_COMMIT ? pushAfterCommit(creations) : pushOnConnection(creations);
                            long end = System.nanoTime();
                            creations.forEach(creation -> committedAt.put(creation.identifier(), end));
                            if (measuring.get()) {
                                commitTimes.add((end - committing) / 1e6);
                                times.add((end - begin) / 1e6);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        failures.add(e);
                    } finally {
                        done.countDown();
                    }
                }));
            }
            Thread.sleep(warmup);
            backlogAtStart = backlog();
            measuring.set(true);
            WaitSampler sampler = sampleWaits ? WaitSampler.start(url(database), WAITS) : null;
            WaitSampler consumers = sampleWaits ? WaitSampler.start(url(database), CONSUMER_WAITS) : null;
            Listener listener = new Listener(url(database));
            long flushesBefore = walFlushes(), completedBefore = completed.get(), start = System.nanoTime();
            Thread.sleep(measure);
            measuring.set(false);
            double seconds = (System.nanoTime() - start) / 1e9;
            long flushes = walFlushes() - flushesBefore, finished = completed.get() - completedBefore;
            long notifications = listener.close();
            List<String> waits = List.of(), consumerWaits = List.of();
            if (sampler != null) {
                waits = sampler.summary().stream().limit(4).map(String::strip).toList();
                sampler.close();
            }
            if (consumers != null) {
                consumerWaits = consumers.summary().stream().limit(3).map(String::strip).toList();
                consumers.close();
            }
            stop.set(true);
            done.await();
            running.forEach(thread -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            if (!failures.isEmpty()) {
                throw new IllegalStateException("A producer failed", failures.getFirst());
            }
            List<Double> commitTimes = commits.stream().flatMap(List::stream).toList();
            return new Load(commitTimes.size(), seconds, Latency.of(commitTimes), Latency.of(transactionTimes.stream().flatMap(List::stream).toList()),
                flushes, notifications, finished, backlogAtStart, backlog(), waits, consumerWaits);
        }

        /**
         * As an application would with plain JDBC: a bound transaction, so the push joins it and the
         * engine notifies after the commit. Returns when the callback returned, so the commit time
         * covers the commit and the engine's after-commit work.
         */
        private long pushAfterCommit(List<Creation<String>> creations) throws Exception {
            long[] committing = new long[1];
            transactions.inTransaction(connection -> {
                try (PreparedStatement insert = connection.prepareStatement(INSERT_EVENT)) {
                    insert.setString(1, "event");
                    insert.executeUpdate();
                }
                pusher.tasks().push(TOPIC, Insertion.APPEND, creations);
                committing[0] = System.nanoTime();
                return null;
            });
            return committing[0];
        }

        /** A push on an explicit connection: the insert trigger (if there is one) notifies inside the transaction. */
        private long pushOnConnection(List<Creation<String>> creations) throws SQLException {
            try (Connection connection = producers.getConnection(); PreparedStatement insert = connection.prepareStatement(INSERT_EVENT)) {
                insert.setString(1, "event");
                insert.executeUpdate();
                pusher.tasks().on(connection).push(TOPIC, Insertion.APPEND, creations);
                long committing = System.nanoTime();
                connection.commit();
                return committing;
            }
        }

        void awaitHandled(Duration timeout) throws InterruptedException {
            long end = System.nanoTime() + timeout.toNanos();
            while (startedAt.size() < committedAt.size() && System.nanoTime() < end) {
                Thread.sleep(50);
            }
        }

        /** Commit returned to handler started, in ms, for every task handled. */
        List<Double> latencies() {
            List<Double> latencies = new ArrayList<>();
            committedAt.forEach((identifier, committed) -> {
                Long started = startedAt.get(identifier);
                if (started != null) {
                    latencies.add((started - committed) / 1e6);
                }
            });
            if (latencies.size() < committedAt.size()) {
                log("  %,d of %,d tasks were not handled before the run ended", committedAt.size() - latencies.size(), committedAt.size());
            }
            return latencies;
        }

        private long backlog() throws SQLException {
            try (Connection connection = pools.getFirst().getConnection(); Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql("SELECT count(*) FROM ${schema}.queue"))) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }

        private long walFlushes() throws SQLException {
            // wal_sync counts fsync and fdatasync calls; with an open_*sync method every write is a flush.
            try (Connection connection = pools.getFirst().getConnection(); Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("""
                     SELECT CASE WHEN current_setting('wal_sync_method') LIKE 'open%' THEN wal_write ELSE wal_sync END FROM pg_stat_wal
                     """)) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }

        @Override
        public void close() throws Exception {
            producers.close();
            for (TaskEngine engine : engines.reversed()) {
                engine.close();
            }
            pools.forEach(HikariDataSource::close);
        }
    }

    /**
     * What the producers' backends are doing: the wait event, and for a lock wait, what lock. A
     * NOTIFY at commit waits for the lock on object 0 of pg_database.
     */
    private static final String WAITS = """
        SELECT coalesce(a.wait_event_type || ':' || a.wait_event, 'CPU')
               || coalesce(' (' || l.locktype || ' ' || l.classid::regclass || ' ' || l.objid || ')', '')
        FROM pg_stat_activity a
        LEFT JOIN pg_locks l ON l.pid = a.pid AND NOT l.granted
        WHERE a.application_name = '%s' AND a.state = 'active' AND a.pid <> pg_backend_pid()
        """.formatted(PRODUCER);

    /** What the engine nodes' backends are doing: the statement, and what it waits on. */
    private static final String CONSUMER_WAITS = """
        SELECT CASE WHEN a.query LIKE '%picked%' THEN 'claim' WHEN a.query LIKE '%DELETE FROM ${schema}.queue%' THEN 'complete'
                    ELSE left(regexp_replace(a.query, '\\s+', ' ', 'g'), 40) END
               || ' | ' || coalesce(a.wait_event_type || ':' || a.wait_event, 'CPU')
        FROM pg_stat_activity a
        WHERE a.application_name LIKE 'pgtasks-bench-node-%' AND a.state = 'active' AND a.pid <> pg_backend_pid()
        """.replace("${schema}", SCHEMA);

    /** Counts the ready notifications sent while it listens. */
    private static final class Listener {

        private final AtomicLong received = new AtomicLong();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Thread thread;

        Listener(String url) throws SQLException {
            Connection connection = DriverManager.getConnection(url, "postgres", null);
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql("LISTEN ${schema}_ready"));
            }
            thread = Thread.ofPlatform().name("notify-counter").daemon().start(() -> {
                try (connection) {
                    PGConnection pg = connection.unwrap(PGConnection.class);
                    while (running.get()) {
                        PGNotification[] notifications = pg.getNotifications(100);
                        if (notifications != null) {
                            received.addAndGet(notifications.length);
                        }
                    }
                } catch (SQLException e) {
                    log("notification counter stopped: %s", e);
                }
            });
        }

        long close() throws InterruptedException {
            running.set(false);
            thread.join(2_000);
            return received.get();
        }
    }

    // ---------------------------------------------------------------------------------------------

    private String fresh(Variant variant) throws SQLException {
        String database = "notify_run_" + ++runs;
        try (Connection connection = admin(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + database + " TEMPLATE " + TEMPLATE + " STRATEGY FILE_COPY");
        }
        try (Connection connection = DriverManager.getConnection(url(database), "postgres", null)) {
            variant.apply(connection);
        }
        // Each run starts after a checkpoint, so none of them pays for one the previous run left.
        try (Connection connection = admin(); Statement statement = connection.createStatement()) {
            statement.execute("CHECKPOINT");
        }
        return database;
    }

    private void drop(String database) throws SQLException {
        try (Connection connection = admin(); Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE " + database + " WITH (FORCE)");
        }
    }

    private Connection admin() throws SQLException {
        return postgres.getPostgresDatabase().getConnection();
    }

    private String url(String database) {
        return "jdbc:postgresql://localhost:" + postgres.getPort() + "/" + database;
    }

    private HikariDataSource pool(String database, String application, int size, boolean autoCommit) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url(database));
        config.setUsername("postgres");
        config.setMaximumPoolSize(size);
        config.setAutoCommit(autoCommit);
        config.addDataSourceProperty("ApplicationName", application);
        return new HikariDataSource(config);
    }

    private static List<Integer> integers(String property, String fallback) {
        return list(property, fallback).stream().map(Integer::parseInt).toList();
    }

    private static List<Variant> variants(String property, String fallback) {
        return list(property, fallback).stream().map(name -> Variant.valueOf(name.replace('-', '_').toUpperCase(Locale.ROOT))).toList();
    }

    private static List<String> list(String property, String fallback) {
        return Arrays.stream(System.getProperty(property, fallback).split(",")).map(String::strip).filter(each -> !each.isEmpty()).toList();
    }

    /** SQL written with {@code ${schema}} for the schema's name. */
    private static String sql(String text) {
        return text.replace("${schema}", SCHEMA);
    }

    private static void log(String format, Object... arguments) {
        System.out.printf(Locale.ROOT, format + "%n", arguments);
    }
}
