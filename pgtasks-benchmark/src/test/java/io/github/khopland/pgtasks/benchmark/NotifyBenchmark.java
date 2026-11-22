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
 * Does the NOTIFY that a push sends at commit limit how fast the application can commit? Postgres
 * serializes the commits of all transactions that called pg_notify behind one database-wide lock
 * (taken in PreCommit_Notify, held until the commit is flushed), and pgtasks notifies from a
 * trigger inside the application's own transaction. With durability on, that could turn every
 * push into a commit that can't share a WAL flush with others. This benchmark measures it with the
 * real engine and schema, fsync and synchronous commit on, and writes target/notify-results.md.
 *
 * <p>Three parts, chosen with {@code benchmark.notify} (comma-separated, or {@code all}):
 * <ul>
 *   <li>{@code contention}: N producers, each committing transactions that insert one row into an
 *       application table and push one task with {@code tasks.on(connection)}, while one engine
 *       node consumes. Commits/s, commit latency, how many commits shared a WAL flush, and what the
 *       producers' backends waited on, for each {@link Variant} of the wake-up.</li>
 *   <li>{@code ceiling}: producers flat out against 1 or 3 engine nodes (separate engines with
 *       separate pools in this JVM), pushing one task or 100 per transaction: what completes per
 *       second, and which side is the limit.</li>
 *   <li>{@code latency}: a steady push rate, and the time from a push's commit to its handler
 *       starting, with LISTEN on, with LISTEN off (polling at the default interval), and with the
 *       mitigations.</li>
 * </ul>
 *
 * <pre>
 * mvn -Pbenchmark -pl pgtasks-benchmark test -Dtest=NotifyBenchmark -Dbenchmark.notify=all
 * </pre>
 * Other system properties: {@code benchmark.producers} (default 1,8,32,64,128),
 * {@code benchmark.variants} (default trigger,none,conditional,debounced), {@code benchmark.seconds}
 * (measured seconds per contention and ceiling run, default 15) after {@code benchmark.warmup}
 * (default 3), {@code benchmark.repeats} (contention rounds, default 1), {@code benchmark.consumer} (default true; false runs the producers alone),
 * {@code benchmark.debounce} (the debounced variant's interval, default 10ms),
 * {@code benchmark.history} (finished tasks in the topic before each run, default 200000),
 * {@code benchmark.nodes} (default 1,3), {@code benchmark.rates} (latency runs' tasks/s, default
 * 100,1000), {@code benchmark.latency-seconds} (default 30; LISTEN off runs four times as long),
 * {@code benchmark.postgres.config} (extra server settings, {@code k=v;k=v}, such as
 * {@code wal_sync_method=fsync_writethrough} for a real flush on macOS) and
 * {@code benchmark.schema} (default the library's, {@code pgtasks}) and {@code benchmark.data-directory}.
 */
@EnabledIfSystemProperty(named = "benchmark.notify", matches = ".+")
class NotifyBenchmark {

    private static final TopicRef<String> TOPIC = TopicRef.of("bench", String.class);
    private static final String TEMPLATE = "notify_template";
    /** The schema the tasks live in, the library's default unless {@code benchmark.schema} says otherwise. */
    private static final String SCHEMA = TaskSchema.validName(System.getProperty("benchmark.schema", TaskSchema.DEFAULT_SCHEMA));
    private static final String PRODUCER = "pgtasks-bench-producer";

    private static final List<String> PARTS = list("benchmark.notify", "all");
    private static final List<Integer> PRODUCERS = list("benchmark.producers", "1,8,32,64,128").stream().map(Integer::parseInt).toList();
    private static final List<Variant> VARIANTS = list("benchmark.variants", "trigger,none,conditional,debounced").stream()
        .map(name -> Variant.valueOf(name.toUpperCase(Locale.ROOT))).toList();
    private static final Duration WARMUP = Duration.ofSeconds(Long.getLong("benchmark.warmup", 3));
    private static final Duration MEASURE = Duration.ofSeconds(Long.getLong("benchmark.seconds", 15));
    private static final boolean CONSUMER = Boolean.parseBoolean(System.getProperty("benchmark.consumer", "true"));
    private static final Duration DEBOUNCE = Duration.ofMillis(Long.parseLong(System.getProperty("benchmark.debounce", "10ms").replace("ms", "")));
    private static final List<Integer> NODES = list("benchmark.nodes", "1,3").stream().map(Integer::parseInt).toList();
    private static final List<Integer> RATES = list("benchmark.rates", "100,1000").stream().map(Integer::parseInt).toList();
    private static final int REPEATS = Integer.getInteger("benchmark.repeats", 1);
    private static final long HISTORY = Long.getLong("benchmark.history", 200_000);
    private static final Duration LATENCY = Duration.ofSeconds(Long.getLong("benchmark.latency-seconds", 30));
    /** The contention runs' consumer polls this often, so the baseline without wake-ups still drains. */
    private static final Duration CONTENTION_POLL = Duration.ofSeconds(1);

    /** How workers learn about new tasks. */
    enum Variant {
        /** R__notify_ready.sql as shipped: every push notifies, inside the application's transaction. */
        TRIGGER("trigger as it is"),
        /** The insert trigger dropped, nothing notifies: the baseline. */
        NONE("no notify (baseline)"),
        /**
         * The insert trigger notifies only when the topic had no ready task before this statement.
         * A busy topic's workers are claiming anyway; an idle topic's wake up as before.
         */
        CONDITIONAL("notify only into an idle topic"),
        /**
         * The insert trigger dropped; after each commit the producer tells a per-node debouncer,
         * which sends one pg_notify on its own connection at once, then at most one per interval.
         */
        DEBOUNCED("debounced notify after commit");

        final String label;

        Variant(String label) {
            this.label = label;
        }

        void apply(Connection connection) throws SQLException {
            try (Statement statement = connection.createStatement()) {
                switch (this) {
                    case TRIGGER -> { }
                    case NONE, DEBOUNCED -> statement.execute(sql("DROP TRIGGER queue_insert_notify ON ${schema}.queue"));
                    case CONDITIONAL -> {
                        // The statement's own rows are visible to an AFTER trigger, so they are
                        // excluded. Rows other transactions haven't committed aren't, so two pushes
                        // into an idle topic at once may both notify, which is harmless.
                        statement.execute(sql("""
                            CREATE OR REPLACE FUNCTION ${schema}.notify_ready_if_idle()
                            RETURNS trigger
                            LANGUAGE plpgsql
                            AS $$
                            BEGIN
                                PERFORM pg_notify('${schema}_ready', ready.topic_id::text)
                                FROM (SELECT DISTINCT topic_id FROM changed_rows WHERE state = 'ready') AS ready
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM ${schema}.queue q
                                    WHERE q.topic_id = ready.topic_id AND q.state = 'ready'
                                      AND q.task_id <> ALL (ARRAY(SELECT task_id FROM changed_rows)));
                                RETURN NULL;
                            END
                            $$"""));
                        statement.execute(sql("""
                            CREATE OR REPLACE TRIGGER queue_insert_notify
                                AFTER INSERT ON ${schema}.queue
                                REFERENCING NEW TABLE AS changed_rows
                                FOR EACH STATEMENT EXECUTE FUNCTION ${schema}.notify_ready_if_idle()"""));
                    }
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
            report.append(String.format(Locale.ROOT, "%nWarm-up %d s, measured %d s per run; debounce %d ms.%n",
                WARMUP.toSeconds(), MEASURE.toSeconds(), DEBOUNCE.toMillis()));
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
                // Without statistics, the planner completes a batch by walking every task of the
                // topic in task_completed_idx, which gets slower with every task completed (see the
                // write-up), and the consumer, not the producers, would set the pace.
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
        try (Run run = new Run(database, variant, CONSUMER ? 1 : 0, 2, CONTENTION_POLL, true)) {
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

    private void ceiling() throws Exception {
        report.append("""

            ## Throughput ceiling

            First the producers push as fast as they can while engine nodes claim and complete (no-op handler, batches \
            of 100); then, on a fresh database, they push at 90 % of the lower of what was pushed and completed, to \
            see whether that rate is sustained: it held if the backlog grew by less than a second's worth. Nodes are \
            separate engines in this JVM, each with its own pool, all on this machine with Postgres.

            | Variant | Nodes | Batches per node | Producers × tasks per transaction | Flat out: pushed/s | completed/s | \
            Paced at /s | completed/s | Backlog at start / end | Held | Commit p50 / p99 ms | Waits (paced) | \
            Consumer waits (paced) |
            |---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---|---|
            """);
        record Shape(int producers, int perTransaction) {
        }
        List<Shape> shapes = List.of(new Shape(64, 1), new Shape(8, 100));
        // As shipped, and with the debounced notify, the mitigation that keeps wake-ups for every push.
        List<Variant> variants = VARIANTS.stream().filter(variant -> variant == Variant.TRIGGER || variant == Variant.DEBOUNCED).toList();
        for (Shape shape : shapes) {
            for (int nodes : NODES) {
                for (int concurrency : List.of(2, 8)) {
                    for (Variant variant : variants) {
                        long before = System.nanoTime();
                        Load flat = ceiling(variant, nodes, concurrency, shape.producers(), shape.perTransaction(), 0);
                        double pushed = flat.commits() * shape.perTransaction() / flat.seconds(), completed = flat.completed() / flat.seconds();
                        int target = (int) (0.9 * Math.min(pushed, completed));
                        Load paced = ceiling(variant, nodes, concurrency, shape.producers(), shape.perTransaction(), target);
                        boolean held = paced.backlog() - paced.backlogStart() < target;
                        String row = String.format(Locale.ROOT,
                            "| %s | %d | %d | %d × %d | %,.0f | %,.0f | %,d | %,.0f | %,d / %,d | %s | %.2f / %.2f | %s | %s |",
                            variant.label, nodes, concurrency, shape.producers(), shape.perTransaction(), pushed, completed,
                            target, paced.completed() / paced.seconds(), paced.backlogStart(), paced.backlog(), held ? "yes" : "no",
                            paced.commit().p50(), paced.commit().p99(), String.join("; ", paced.waits()),
                            String.join("; ", paced.consumerWaits()));
                        report.append(row).append('\n');
                        log("%s (%.0f s)", row, (System.nanoTime() - before) / 1e9);
                    }
                }
            }
        }
    }

    private Load ceiling(Variant variant, int nodes, int concurrency, int producers, int perTransaction, int rate) throws Exception {
        String database = fresh(variant);
        try (Run run = new Run(database, variant, nodes, concurrency, null, true)) {
            return run.load(producers, perTransaction, rate, WARMUP, MEASURE, true);
        } finally {
            drop(database);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Part 3: end-to-end latency

    private void latency() throws Exception {
        report.append("""

            ## End-to-end latency

            8 producers at a steady total rate, one task per transaction; one engine node, default settings (2 batches \
            of 100, 30 s poll). Latency is from the producer's commit returning to the handler starting, in ms.

            | Wake-up | Rate/s | Seconds | Tasks | p50 | p99 | p99.9 | max | Over 1 s |
            |---|---:|---:|---:|---:|---:|---:|---:|---:|
            """);
        record Case(Variant variant, boolean listen) {
            String label() {
                return listen ? variant.label + ", LISTEN on" : "LISTEN off (poll only)";
            }
        }
        List<Case> cases = new ArrayList<>();
        cases.add(new Case(Variant.TRIGGER, true));
        cases.add(new Case(Variant.TRIGGER, false));
        VARIANTS.stream().filter(variant -> variant == Variant.CONDITIONAL || variant == Variant.DEBOUNCED)
            .forEach(variant -> cases.add(new Case(variant, true)));
        for (int rate : RATES) {
            for (Case each : cases) {
                Duration length = each.listen() ? LATENCY : LATENCY.multipliedBy(4);
                String database = fresh(each.variant());
                try (Run run = new Run(database, each.variant(), 1, 2, null, each.listen())) {
                    run.load(8, 1, rate, Duration.ZERO, length, false);
                    // Let the last ones be handled, up to one poll interval.
                    run.awaitHandled(Duration.ofSeconds(35));
                    List<Double> latencies = run.latencies();
                    Latency latency = Latency.of(latencies);
                    long slow = latencies.stream().filter(value -> value > 1_000).count();
                    String row = String.format(Locale.ROOT, "| %s | %,d | %d | %,d | %.1f | %.1f | %.1f | %,.0f | %,d |",
                        each.label(), rate, length.toSeconds(), latencies.size(), latency.p50(), latency.p99(), latency.p999(), latency.max(), slow);
                    report.append(row).append('\n');
                    log("%s", row);
                }
                drop(database);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** One database with engine nodes consuming from it, producers pushing into it and a debouncer if the variant has one. */
    private final class Run implements AutoCloseable {

        private final String database;
        private final Variant variant;
        private final List<HikariDataSource> pools = new ArrayList<>();
        private final List<TaskEngine> engines = new ArrayList<>();
        private final HikariDataSource producers;
        private final Debouncer debouncer;
        private final AtomicLong completed = new AtomicLong();
        private final Map<String, Long> committedAt = new ConcurrentHashMap<>();
        private final Map<String, Long> startedAt = new ConcurrentHashMap<>();
        private final TaskEngine pusher;
        private long backlogAtStart;

        Run(String database, Variant variant, int nodes, int concurrency, Duration poll, boolean listen) throws Exception {
            this.database = database;
            this.variant = variant;
            for (int node = 1; node <= Math.max(nodes, 1); node++) {
                HikariDataSource pool = pool(database, "pgtasks-bench-node-" + node, 2 * concurrency + 6, true);
                pools.add(pool);
                TaskEngine engine = TaskEngine.builder()
                    .dataSource(pool)
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
                // With no consumer, one engine is still needed to push; it is prepared, never started.
                engines.add(nodes == 0 ? engine.prepare() : engine.start());
            }
            pusher = engines.getFirst();
            producers = pool(database, PRODUCER, 160, false);
            debouncer = variant == Variant.DEBOUNCED ? new Debouncer(url(database), topicId(pools.getFirst())) : null;
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
            List<List<Double>> commits = new ArrayList<>(), transactions = new ArrayList<>();
            List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch done = new CountDownLatch(threads);
            List<Thread> running = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                List<Double> commitTimes = new ArrayList<>(), transactionTimes = new ArrayList<>();
                commits.add(commitTimes);
                transactions.add(transactionTimes);
                long every = rate == 0 ? 0 : 1_000_000_000L * threads * perTransaction / rate;
                long offset = every == 0 ? 0 : every * thread / threads;
                running.add(Thread.ofPlatform().name("producer-" + thread).start(() -> {
                    try (Connection connection = producers.getConnection();
                         PreparedStatement insert = connection.prepareStatement("INSERT INTO app.event (body) VALUES (?)")) {
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
                            insert.setString(1, "event");
                            insert.executeUpdate();
                            pusher.tasks().on(connection).push(TOPIC, Insertion.APPEND, creations);
                            long committing = System.nanoTime();
                            connection.commit();
                            long end = System.nanoTime();
                            creations.forEach(creation -> committedAt.put(creation.identifier(), end));
                            if (debouncer != null) {
                                debouncer.pushed();
                            }
                            if (measuring.get()) {
                                commitTimes.add((end - committing) / 1e6);
                                transactionTimes.add((end - begin) / 1e6);
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
            return new Load(commitTimes.size(), seconds, Latency.of(commitTimes), Latency.of(transactions.stream().flatMap(List::stream).toList()),
                flushes, notifications, finished, backlogAtStart, backlog(), waits, consumerWaits);
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
            if (debouncer != null) {
                debouncer.close();
            }
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

    /**
     * The debounced variant's notifier, one per node: the first push after a quiet spell notifies at
     * once, later ones within the interval share one notification at its end. It notifies in its
     * own autocommit transaction, so only its commits queue for the notify lock.
     */
    private static final class Debouncer implements AutoCloseable {

        private final Connection connection;
        private final int topicId;
        private final Thread thread;
        private boolean pending;
        private boolean running = true;

        Debouncer(String url, int topicId) throws SQLException {
            this.connection = DriverManager.getConnection(url, "postgres", null);
            this.topicId = topicId;
            this.thread = Thread.ofPlatform().name("debouncer").daemon().start(this::run);
        }

        synchronized void pushed() {
            if (!pending) {
                pending = true;
                notifyAll();
            }
        }

        private void run() {
            try (PreparedStatement notify = connection.prepareStatement(sql("SELECT pg_notify('${schema}_ready', ?)"))) {
                notify.setString(1, Integer.toString(topicId));
                while (true) {
                    synchronized (this) {
                        while (!pending && running) {
                            wait();
                        }
                        if (!running) {
                            return;
                        }
                        pending = false;
                    }
                    long sent = System.nanoTime();
                    notify.execute();
                    long rest = DEBOUNCE.toNanos() - (System.nanoTime() - sent);
                    if (rest > 0) {
                        Thread.sleep(Duration.ofNanos(rest));
                    }
                }
            } catch (SQLException | InterruptedException e) {
                log("debouncer stopped: %s", e);
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (this) {
                running = false;
                notifyAll();
            }
            thread.join(2_000);
            connection.close();
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

    private static int topicId(HikariDataSource pool) throws SQLException {
        try (Connection connection = pool.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql("SELECT id FROM ${schema}.topic WHERE name = ?"))) {
            statement.setString(1, TOPIC.name());
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1);
            }
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
