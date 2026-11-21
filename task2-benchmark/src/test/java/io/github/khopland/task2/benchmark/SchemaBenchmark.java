package io.github.khopland.task2.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import io.github.khopland.task2.Ordering;
import org.junit.jupiter.api.Test;

/**
 * Phase 2 benchmark: the queue-and-task schema against a single-table design, at several history
 * sizes, with four workers claiming batches of 100 and completing them at once. Writes a
 * markdown report to target/benchmark-results.md.
 *
 * <p>System properties: {@code benchmark.history} (comma-separated history sizes),
 * {@code benchmark.designs} ({@code queue}, {@code single}), {@code benchmark.scenarios}
 * (substrings of scenario names, or {@code =name} for one exactly; setup runs regardless, so the
 * state a scenario starts from is the same), and {@code benchmark.diagnose}: samples wait events, logs throughput per second, and
 * has Postgres log the plans of statements over 200 ms ({@code benchmark.explain-over}),
 * autovacuum runs and lock waits to target/postgres-logs. Diagnosis costs throughput, so its numbers aren't the headline ones.
 */
class SchemaBenchmark {

    private static final int WORKERS = 4;
    private static final int TASKS = 20_000;
    private static final boolean DIAGNOSE = Boolean.getBoolean("benchmark.diagnose");
    private static final List<String> DESIGNS = List.of(System.getProperty("benchmark.designs", "queue,single").split(","));
    private static final List<String> SCENARIOS = Arrays.stream(System.getProperty("benchmark.scenarios", "").split(",")).filter(each -> !each.isBlank()).toList();

    record Result(String design, long history, String scenario, int tasks, double seconds, Latency claims, Latency completions, long emptyClaims, long bytes) {

        String row() {
            return String.format(Locale.ROOT, "| %s | %,d | %s | %,.0f | %.2f | %.2f | %s | %,d | %,d MB |",
                design, history, scenario, tasks / seconds, claims.p50(), claims.p99(),
                completions == null ? "" : String.format(Locale.ROOT, "%.2f / %.2f", completions.p50(), completions.p99()),
                emptyClaims, bytes / 1_000_000);
        }
    }

    record Latency(double p50, double p99) {

        static Latency of(List<Double> milliseconds) {
            List<Double> sorted = new ArrayList<>(milliseconds);
            Collections.sort(sorted);
            return new Latency(sorted.get(sorted.size() / 2), sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * 0.99))));
        }
    }

    @Test
    void compare() throws Exception {
        List<Long> histories = Arrays.stream(System.getProperty("benchmark.history", "0,1000000").split(",")).map(Long::parseLong).toList();
        List<Result> results = new ArrayList<>();
        EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder()
            .setServerConfig("shared_buffers", "1GB")
            .setServerConfig("work_mem", "16MB")
            .setServerConfig("maintenance_work_mem", "512MB")
            .setServerConfig("max_wal_size", "8GB")
            .setServerConfig("checkpoint_timeout", "30min")
            .setServerConfig("fsync", "off")
            .setServerConfig("synchronous_commit", "off")
            .setServerConfig("full_page_writes", "off");
        if (DIAGNOSE) {
            Path logs = Path.of("target", "postgres-logs").toAbsolutePath();
            Files.createDirectories(logs);
            builder.setServerConfig("shared_preload_libraries", "auto_explain")
                .setServerConfig("auto_explain.log_min_duration", System.getProperty("benchmark.explain-over", "200ms"))
                .setServerConfig("auto_explain.log_analyze", "on")
                .setServerConfig("auto_explain.log_buffers", "on")
                .setServerConfig("auto_explain.log_nested_statements", "on")
                .setServerConfig("logging_collector", "on")
                .setServerConfig("log_directory", logs.toString())
                .setServerConfig("log_filename", "postgres.log")
                .setServerConfig("log_autovacuum_min_duration", "0")
                .setServerConfig("log_lock_waits", "on")
                .setServerConfig("deadlock_timeout", "100ms")
                .setServerConfig("track_io_timing", "on");
            log("Diagnosing: Postgres logs plans over %s, autovacuum and lock waits to %s", System.getProperty("benchmark.explain-over", "200ms"), logs);
        }
        try (EmbeddedPostgres postgres = builder.start()) {
            for (long history : histories) {
                for (Function<DataSource, Design> design : designs()) {
                    String database = "bench_" + history + "_" + results.size();
                    try (var admin = postgres.getPostgresDatabase().getConnection(); var statement = admin.createStatement()) {
                        statement.execute("CREATE DATABASE " + database);
                    }
                    try (HikariDataSource pool = pool(postgres, database)) {
                        results.addAll(run(design.apply(pool), history, pool.getJdbcUrl()));
                    }
                    try (var admin = postgres.getPostgresDatabase().getConnection(); var statement = admin.createStatement()) {
                        statement.execute("DROP DATABASE " + database);
                    }
                }
            }
        }
        report(histories, results);
    }

    private static List<Function<DataSource, Design>> designs() {
        List<Function<DataSource, Design>> designs = new ArrayList<>();
        if (DESIGNS.contains("queue")) {
            designs.add(QueueDesign::new);
        }
        if (DESIGNS.contains("single")) {
            designs.add(SingleTableDesign::new);
        }
        return designs;
    }

    private static boolean selected(String scenario) {
        return SCENARIOS.isEmpty() || SCENARIOS.stream().anyMatch(each -> each.startsWith("=") ? scenario.equals(each.substring(1)) : scenario.contains(each));
    }

    private static HikariDataSource pool(EmbeddedPostgres postgres, String database) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:" + postgres.getPort() + "/" + database);
        config.setUsername("postgres");
        config.setMaximumPoolSize(WORKERS * 2);
        return new HikariDataSource(config);
    }

    private static List<Result> run(Design design, long history, String url) throws Exception {
        log("%s with %,d history rows: setting up", design.name(), history);
        design.create();
        long started = System.nanoTime();
        if (history > 0) {
            design.history(history);
        }
        design.vacuum();
        log("  history generated in %.0f s, %,d MB", (System.nanoTime() - started) / 1e9, design.bytes() / 1_000_000);
        List<Result> results = new ArrayList<>();
        if (selected("idle poll")) {
            results.add(idlePolls(design, history));
        }
        Drainer drainer = new Drainer(design, history, url, results);
        design.ready(TASKS, "p", 0, 5_000);
        drainer.drain("FIFO", Ordering.FIFO, TASKS, false);
        // Statistics decide the claim plans, so both SINGULAR runs fix them instead of leaving
        // them to autovacuum's timing: first taken with the tasks queued, then taken right
        // after a drain, which is what autovacuum usually sees on a queue.
        design.ready(TASKS, "s", 0, 5_000);
        design.analyze();
        drainer.drain("SINGULAR", Ordering.SINGULAR_BY_IDENTIFIER, TASKS, false);
        design.analyze();
        design.ready(TASKS, "e", 0, 5_000);
        drainer.drain("SINGULAR after analyzing an empty queue", Ordering.SINGULAR_BY_IDENTIFIER, TASKS, false);
        design.deadLetters(100_000);
        design.vacuum();
        design.ready(TASKS, "f", 0, 5_000);
        drainer.drain("ON_FAILURE, 100k dead letters", Ordering.SINGULAR_BY_IDENTIFIER_ON_FAILURE, TASKS, false);
        design.ready(2_000, "hot", 0, 1);
        drainer.drain("one hot identifier, SINGULAR", Ordering.SINGULAR_BY_IDENTIFIER, 2_000, false);
        if (design instanceof QueueDesign) {
            design.ready(TASKS, "r", 0, 5_000);
            drainer.drain("FIFO with strict admission", Ordering.FIFO, TASKS, true);
        }
        return results;
    }

    /** Drains the ready tasks of a scenario if it is selected, and otherwise just clears them, so later scenarios start from the same state. */
    private record Drainer(Design design, long history, String url, List<Result> results) {

        void drain(String scenario, Ordering ordering, int tasks, boolean admission) throws Exception {
            if (selected(scenario)) {
                results.add(SchemaBenchmark.drain(design, history, url, scenario, ordering, tasks, admission));
            } else {
                SchemaBenchmark.drain(design, history, null, scenario, ordering, tasks, admission);
            }
        }
    }

    /** Polls against an empty queue: what an idle topic with this much history costs. */
    private static Result idlePolls(Design design, long history) throws Exception {
        List<Double> latencies = new ArrayList<>();
        long started = System.nanoTime();
        for (int poll = 0; poll < 1_000; poll++) {
            long before = System.nanoTime();
            design.claim(1, Ordering.SINGULAR_BY_IDENTIFIER, 100, false);
            latencies.add((System.nanoTime() - before) / 1e6);
        }
        double seconds = (System.nanoTime() - started) / 1e9;
        return result(design, history, "idle poll (1,000 empty claims)", 1_000, seconds, latencies, null, 1_000);
    }

    /** @param url the benchmark database, to sample when diagnosing; null for a run that only clears the tasks */
    private static Result drain(Design design, long history, String url, String scenario, Ordering ordering, int tasks, boolean admission) throws Exception {
        AtomicInteger completed = new AtomicInteger();
        AtomicInteger empty = new AtomicInteger();
        List<Double> latencies = Collections.synchronizedList(new ArrayList<>());
        List<Double> completions = Collections.synchronizedList(new ArrayList<>());
        long deadline = System.nanoTime() + Duration.ofMinutes(10).toNanos();
        boolean diagnose = DIAGNOSE && url != null;
        WaitSampler sampler = diagnose ? WaitSampler.start(url) : null;
        List<Integer> perSecond = new ArrayList<>();
        Thread ticker = diagnose ? Thread.ofPlatform().daemon().start(() -> {
            int last = 0;
            try {
                while (true) {
                    Thread.sleep(1_000);
                    int now = completed.get();
                    perSecond.add(now - last);
                    last = now;
                }
            } catch (InterruptedException ignored) {
            }
        }) : null;
        if (diagnose) {
            mark(url, "benchmark scenario start: " + design.name() + " / " + scenario);
        }
        long started = System.nanoTime();
        try (ExecutorService workers = Executors.newFixedThreadPool(WORKERS)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int worker = 0; worker < WORKERS; worker++) {
                futures.add(workers.submit(() -> {
                    while (completed.get() < tasks && System.nanoTime() < deadline) {
                        long before = System.nanoTime();
                        List<Design.Claimed> claimed = design.claim(1, ordering, 100, admission);
                        latencies.add((System.nanoTime() - before) / 1e6);
                        if (claimed.isEmpty()) {
                            empty.incrementAndGet();
                            Thread.sleep(1);
                            continue;
                        }
                        long completing = System.nanoTime();
                        design.complete(1, claimed);
                        completions.add((System.nanoTime() - completing) / 1e6);
                        completed.addAndGet(claimed.size());
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
        double seconds = (System.nanoTime() - started) / 1e9;
        if (diagnose) {
            mark(url, "benchmark scenario end");
            ticker.interrupt();
            sampler.close();
        }
        if (completed.get() < tasks) {
            throw new IllegalStateException(scenario + " did not finish: " + completed.get() + " of " + tasks);
        }
        if (url == null) {
            log("  %s: cleared %,d tasks in %.1f s (not selected)", scenario, tasks, seconds);
            return null;
        }
        Result result = result(design, history, scenario, tasks, seconds, latencies, completions, empty.get());
        if (diagnose) {
            log("    completed per second: %s", perSecond);
            log("    backends by state (average count over %.0f s):", seconds);
            sampler.summary().forEach(line -> log("      %s", line));
        }
        return result;
    }

    /** Writes a line to the Postgres log, so its plans can be told apart by scenario. */
    private static void mark(String url, String text) throws java.sql.SQLException {
        try (var connection = java.sql.DriverManager.getConnection(url, "postgres", null);
             var statement = connection.prepareStatement("SELECT set_config('benchmark.mark', ?, false)")) {
            statement.setString(1, text);
            statement.execute();
            try (var raise = connection.createStatement()) {
                raise.execute("DO $$ BEGIN RAISE LOG '%', current_setting('benchmark.mark'); END $$");
            }
        }
    }

    private static Result result(Design design, long history, String scenario, int tasks, double seconds, List<Double> claims, List<Double> completions, long empty) throws Exception {
        Result result = new Result(design.name(), history, scenario, tasks, seconds, Latency.of(claims),
            completions == null || completions.isEmpty() ? null : Latency.of(completions), empty, design.bytes());
        log("  %s", result.row());
        return result;
    }

    private static void report(List<Long> histories, List<Result> results) throws IOException {
        StringBuilder report = new StringBuilder("""
            | Design | History rows | Scenario | Tasks/s | Claim p50 ms | Claim p99 ms | Complete p50 / p99 ms | Empty claims | Size after |
            |---|---:|---|---:|---:|---:|---:|---:|---:|
            """);
        results.forEach(result -> report.append(result.row()).append('\n'));
        Path file = Path.of("target", "benchmark-results.md");
        Files.writeString(file, "History sizes: " + histories + "\n\n" + report);
        log("%n%s", report);
    }

    private static void log(String format, Object... arguments) {
        System.out.printf(Locale.ROOT, format + "%n", arguments);
    }
}
