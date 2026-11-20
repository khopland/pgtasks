package org.example.task2.benchmark;

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
import org.example.task2.Ordering;
import org.junit.jupiter.api.Test;

/**
 * Phase 2 benchmark: the queue-and-task schema against a single-table design, at several history
 * sizes, with four workers claiming batches of 100 and completing them at once. Writes a
 * markdown report to target/benchmark-results.md.
 */
class SchemaBenchmark {

    private static final int WORKERS = 4;
    private static final int TASKS = 20_000;

    record Result(String design, long history, String scenario, int tasks, double seconds, double p50, double p99, long emptyClaims, long bytes) {

        String row() {
            return String.format(Locale.ROOT, "| %s | %,d | %s | %,.0f | %.2f | %.2f | %,d | %,d MB |",
                design, history, scenario, tasks / seconds, p50, p99, emptyClaims, bytes / 1_000_000);
        }
    }

    @Test
    void compare() throws Exception {
        List<Long> histories = Arrays.stream(System.getProperty("benchmark.history", "0,1000000").split(",")).map(Long::parseLong).toList();
        List<Result> results = new ArrayList<>();
        try (EmbeddedPostgres postgres = EmbeddedPostgres.builder()
            .setServerConfig("shared_buffers", "1GB")
            .setServerConfig("work_mem", "16MB")
            .setServerConfig("maintenance_work_mem", "512MB")
            .setServerConfig("max_wal_size", "8GB")
            .setServerConfig("checkpoint_timeout", "30min")
            .setServerConfig("fsync", "off")
            .setServerConfig("synchronous_commit", "off")
            .setServerConfig("full_page_writes", "off")
            .start()) {
            for (long history : histories) {
                for (Function<DataSource, Design> design : List.<Function<DataSource, Design>>of(QueueDesign::new, SingleTableDesign::new)) {
                    String database = "bench_" + history + "_" + results.size();
                    try (var admin = postgres.getPostgresDatabase().getConnection(); var statement = admin.createStatement()) {
                        statement.execute("CREATE DATABASE " + database);
                    }
                    try (HikariDataSource pool = pool(postgres, database)) {
                        results.addAll(run(design.apply(pool), history));
                    }
                    try (var admin = postgres.getPostgresDatabase().getConnection(); var statement = admin.createStatement()) {
                        statement.execute("DROP DATABASE " + database);
                    }
                }
            }
        }
        report(histories, results);
    }

    private static HikariDataSource pool(EmbeddedPostgres postgres, String database) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:" + postgres.getPort() + "/" + database);
        config.setUsername("postgres");
        config.setMaximumPoolSize(WORKERS * 2);
        return new HikariDataSource(config);
    }

    private static List<Result> run(Design design, long history) throws Exception {
        log("%s with %,d history rows: setting up", design.name(), history);
        design.create();
        long started = System.nanoTime();
        if (history > 0) {
            design.history(history);
        }
        design.vacuum();
        log("  history generated in %.0f s, %,d MB", (System.nanoTime() - started) / 1e9, design.bytes() / 1_000_000);
        List<Result> results = new ArrayList<>();
        results.add(idlePolls(design, history));
        design.ready(TASKS, "p", 5_000);
        results.add(drain(design, history, "FIFO", Ordering.FIFO, TASKS, false));
        design.ready(TASKS, "s", 5_000);
        results.add(drain(design, history, "SINGULAR", Ordering.SINGULAR_BY_IDENTIFIER, TASKS, false));
        design.deadLetters(100_000);
        design.vacuum();
        design.ready(TASKS, "f", 5_000);
        results.add(drain(design, history, "ON_FAILURE, 100k dead letters", Ordering.SINGULAR_BY_IDENTIFIER_ON_FAILURE, TASKS, false));
        design.ready(2_000, "hot", 1);
        results.add(drain(design, history, "one hot identifier, SINGULAR", Ordering.SINGULAR_BY_IDENTIFIER, 2_000, false));
        if (design instanceof QueueDesign) {
            design.ready(TASKS, "r", 5_000);
            results.add(drain(design, history, "FIFO with strict admission", Ordering.FIFO, TASKS, true));
        }
        return results;
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
        return result(design, history, "idle poll (1,000 empty claims)", 1_000, seconds, latencies, 1_000);
    }

    private static Result drain(Design design, long history, String scenario, Ordering ordering, int tasks, boolean admission) throws Exception {
        AtomicInteger completed = new AtomicInteger();
        AtomicInteger empty = new AtomicInteger();
        List<Double> latencies = Collections.synchronizedList(new ArrayList<>());
        long deadline = System.nanoTime() + Duration.ofMinutes(10).toNanos();
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
                        design.complete(1, claimed);
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
        if (completed.get() < tasks) {
            throw new IllegalStateException(scenario + " did not finish: " + completed.get() + " of " + tasks);
        }
        return result(design, history, scenario, tasks, seconds, latencies, empty.get());
    }

    private static Result result(Design design, long history, String scenario, int tasks, double seconds, List<Double> latencies, long empty) throws Exception {
        List<Double> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        Result result = new Result(design.name(), history, scenario, tasks, seconds,
            sorted.get(sorted.size() / 2), sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * 0.99))), empty, design.bytes());
        log("  %s", result.row());
        return result;
    }

    private static void report(List<Long> histories, List<Result> results) throws IOException {
        StringBuilder report = new StringBuilder("""
            | Design | History rows | Scenario | Tasks/s | Claim p50 ms | Claim p99 ms | Empty claims | Size after |
            |---|---:|---|---:|---:|---:|---:|---:|
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
