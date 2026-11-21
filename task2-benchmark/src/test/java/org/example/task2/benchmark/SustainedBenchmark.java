package org.example.task2.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.example.task2.Ordering;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Proposal 5: the schema under sustained load, before Phase 4 builds on it. The Phase 2 benchmark
 * drained short bursts with durability off; this one pushes at a steady rate for an hour or more,
 * on 25 million rows of history, with fsync and synchronous commit on, while workers claim and
 * complete and retention deletes as many old tasks as were completed, so history stays the same
 * size and the tables see the inserts, updates and deletes of a long-running system. Every minute
 * it records throughput, the backlog, claim and completion latency, table and index sizes, dead
 * rows, autovacuum runs and WAL written, and writes them to target/sustained-results.md.
 *
 * <p>It needs room: 25 million rows take about 8 GB of disk and 4 minutes to generate per design.
 * Run it on a machine with at least 16 GB of memory, not on a small VM:
 * <pre>
 * mvn -Pbenchmark -pl task2-benchmark test -Dtest=SustainedBenchmark -Dbenchmark.sustained=60m
 * </pre>
 * System properties: {@code benchmark.sustained} (how long to load each design, such as 60m or
 * 2h; without it the test is skipped), {@code benchmark.history} (default 25000000),
 * {@code benchmark.rate} (tasks pushed per second, default 1000), {@code benchmark.workers}
 * (default 4), {@code benchmark.designs} (default {@code queue}; {@code queue,single} compares
 * both, one after the other), {@code benchmark.shared-buffers} (default 1GB) and
 * {@code benchmark.data-directory} (default a temporary directory).
 */
@EnabledIfSystemProperty(named = "benchmark.sustained", matches = ".+")
class SustainedBenchmark {

    private static final Duration LENGTH = duration(System.getProperty("benchmark.sustained", "60m"));
    private static final long HISTORY = Long.getLong("benchmark.history", 25_000_000);
    private static final int RATE = Integer.getInteger("benchmark.rate", 1_000);
    private static final int WORKERS = Integer.getInteger("benchmark.workers", 4);
    private static final List<String> DESIGNS = List.of(System.getProperty("benchmark.designs", "queue").split(","));
    private static final Duration MINUTE = Duration.ofMinutes(1);
    private static final Duration PUSH_EVERY = Duration.ofMillis(100);
    private static final Duration PURGE_EVERY = Duration.ofSeconds(10);
    private static final int BATCH = 100;

    /** One table's state at the end of a minute. */
    record Table(String name, long liveRows, long deadRows, long autovacuums, long autoanalyzes, long heapBytes, long indexBytes) {
    }

    /** One minute of load. */
    record Minute(int minute, long pushed, long completed, long backlog, SchemaBenchmark.Latency claims, SchemaBenchmark.Latency completions,
                  long purged, long walBytes, List<Table> tables) {

        String row() {
            StringBuilder row = new StringBuilder(String.format(Locale.ROOT, "| %d | %,d | %,d | %,d | %s | %s | %,d | %,d |",
                minute, pushed / 60, completed / 60, backlog, latency(claims), latency(completions), purged, walBytes / 1_000_000));
            for (Table table : tables) {
                row.append(String.format(Locale.ROOT, " %,d / %,d MB | %,d | %d |",
                    table.heapBytes() / 1_000_000, table.indexBytes() / 1_000_000, table.deadRows(), table.autovacuums()));
            }
            return row.toString();
        }

        private static String latency(SchemaBenchmark.Latency latency) {
            return latency == null ? "" : String.format(Locale.ROOT, "%.1f / %.1f", latency.p50(), latency.p99());
        }
    }

    @Test
    void sustained() throws Exception {
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
            .setServerConfig("log_autovacuum_min_duration", "0");
        String directory = System.getProperty("benchmark.data-directory");
        if (directory != null) {
            builder.setDataDirectory(Path.of(directory)).setCleanDataDirectory(true);
        }
        Map<String, List<Minute>> results = new LinkedHashMap<>();
        try (EmbeddedPostgres postgres = builder.start()) {
            for (String name : DESIGNS) {
                Function<DataSource, Design> design = switch (name) {
                    case "queue" -> QueueDesign::new;
                    case "single" -> SingleTableDesign::new;
                    default -> throw new IllegalArgumentException("Unknown design " + name + "; use queue or single");
                };
                String database = "sustained_" + name;
                try (var admin = postgres.getPostgresDatabase().getConnection(); var statement = admin.createStatement()) {
                    statement.execute("CREATE DATABASE " + database);
                }
                try (HikariDataSource pool = pool(postgres, database)) {
                    Design under = design.apply(pool);
                    results.put(under.name(), run(under, pool));
                }
                try (var admin = postgres.getPostgresDatabase().getConnection(); var statement = admin.createStatement()) {
                    statement.execute("DROP DATABASE " + database);
                }
            }
        }
        report(results);
    }

    private static List<Minute> run(Design design, DataSource dataSource) throws Exception {
        log("%s: generating %,d history rows", design.name(), HISTORY);
        long setup = System.nanoTime();
        design.create();
        design.history(HISTORY);
        design.vacuum();
        log("  done in %.0f s, %,d MB; pushing %,d tasks/s for %s with %d workers, durability on",
            (System.nanoTime() - setup) / 1e9, design.bytes() / 1_000_000, RATE, LENGTH, WORKERS);

        AtomicLong pushed = new AtomicLong(), completed = new AtomicLong(), purged = new AtomicLong(), toPurge = new AtomicLong();
        List<Double> claims = new CopyOnWriteArrayList<>(), completions = new CopyOnWriteArrayList<>();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        long end = System.nanoTime() + LENGTH.toNanos();
        List<Minute> minutes = new ArrayList<>();
        try (ExecutorService threads = Executors.newFixedThreadPool(WORKERS + 2)) {
            List<Future<?>> running = new ArrayList<>();
            // The producer: a batch every 100 ms, catching up if a push ran late.
            running.add(threads.submit(() -> {
                int perPush = Math.max(1, (int) (RATE * PUSH_EVERY.toMillis() / 1000));
                long next = System.nanoTime();
                while (System.nanoTime() < end) {
                    design.ready(perPush, "p", 200_000);
                    pushed.addAndGet(perPush);
                    next += PUSH_EVERY.toNanos();
                    long wait = next - System.nanoTime();
                    if (wait > 0) {
                        Thread.sleep(Duration.ofNanos(wait));
                    }
                }
                return null;
            }));
            for (int worker = 0; worker < WORKERS; worker++) {
                running.add(threads.submit(() -> {
                    while (System.nanoTime() < end) {
                        long before = System.nanoTime();
                        List<Design.Claimed> claimed = design.claim(1, Ordering.FIFO, BATCH, false);
                        claims.add((System.nanoTime() - before) / 1e6);
                        if (claimed.isEmpty()) {
                            Thread.sleep(5);
                            continue;
                        }
                        long completing = System.nanoTime();
                        design.complete(1, claimed);
                        completions.add((System.nanoTime() - completing) / 1e6);
                        completed.addAndGet(claimed.size());
                        toPurge.addAndGet(claimed.size());
                    }
                    return null;
                }));
            }
            // Retention: deletes as many of the oldest finished tasks as were completed, so the
            // history stays at its starting size.
            running.add(threads.submit(() -> {
                while (System.nanoTime() < end) {
                    Thread.sleep(PURGE_EVERY);
                    long due = toPurge.getAndSet(0);
                    while (due > 0) {
                        int deleted = design.purgeOldest((int) Math.min(due, 1_000));
                        purged.addAndGet(deleted);
                        due -= Math.max(deleted, 1);
                    }
                }
                return null;
            }));

            long lastPushed = 0, lastCompleted = 0, lastPurged = 0, lastWal = wal(dataSource);
            for (int minute = 1; System.nanoTime() < end; minute++) {
                Thread.sleep(Duration.ofNanos(Math.min(MINUTE.toNanos(), Math.max(0, end - System.nanoTime()))));
                List<Double> claimed = drain(claims), finished = drain(completions);
                long nowPushed = pushed.get(), nowCompleted = completed.get(), nowPurged = purged.get(), nowWal = wal(dataSource);
                Minute row = new Minute(minute, nowPushed - lastPushed, nowCompleted - lastCompleted, nowPushed - nowCompleted,
                    claimed.isEmpty() ? null : SchemaBenchmark.Latency.of(claimed),
                    finished.isEmpty() ? null : SchemaBenchmark.Latency.of(finished),
                    nowPurged - lastPurged, nowWal - lastWal, tables(dataSource, design.tables()));
                minutes.add(row);
                log("  %s", row.row());
                lastPushed = nowPushed;
                lastCompleted = nowCompleted;
                lastPurged = nowPurged;
                lastWal = nowWal;
                running.stream().filter(Future::isDone).forEach(done -> {
                    try {
                        done.get();
                    } catch (Exception e) {
                        failures.add(e);
                    }
                });
                if (!failures.isEmpty()) {
                    break;
                }
            }
            for (Future<?> future : running) {
                future.cancel(true);
            }
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException("The load failed after " + minutes.size() + " minute(s)", failures.getFirst());
        }
        return minutes;
    }

    private static List<Double> drain(List<Double> samples) {
        List<Double> taken = new ArrayList<>(samples);
        samples.removeAll(taken);
        return taken;
    }

    private static long wal(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); var statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT pg_current_wal_lsn() - '0/0'::pg_lsn")) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static List<Table> tables(DataSource dataSource, List<String> names) throws Exception {
        List<Table> tables = new ArrayList<>();
        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("""
                 SELECT n_live_tup, n_dead_tup, autovacuum_count, autoanalyze_count, pg_table_size(relid), pg_indexes_size(relid)
                 FROM pg_stat_user_tables
                 WHERE relid = ?::regclass
                 """)) {
            for (String name : names) {
                statement.setString(1, name);
                try (ResultSet resultSet = statement.executeQuery()) {
                    resultSet.next();
                    tables.add(new Table(name, resultSet.getLong(1), resultSet.getLong(2), resultSet.getLong(3), resultSet.getLong(4),
                        resultSet.getLong(5), resultSet.getLong(6)));
                }
            }
        }
        return tables;
    }

    private static void report(Map<String, List<Minute>> results) throws Exception {
        StringBuilder report = new StringBuilder(String.format(Locale.ROOT,
            "Sustained load: %s per design, %,d history rows, %,d tasks/s pushed, %d workers claiming batches of %d, durability on.%n",
            LENGTH, HISTORY, RATE, WORKERS, BATCH));
        results.forEach((design, minutes) -> {
            report.append("\n## ").append(design).append("\n\n");
            List<String> tables = minutes.isEmpty() ? List.of() : minutes.getFirst().tables().stream().map(Table::name).toList();
            report.append("| Minute | Pushed/s | Completed/s | Backlog | Claim p50 / p99 ms | Complete p50 / p99 ms | Purged | WAL MB |");
            tables.forEach(table -> report.append(" ").append(table).append(" heap / indexes | dead rows | autovacuums |"));
            report.append("\n|---:|---:|---:|---:|---:|---:|---:|---:|").append("---:|---:|---:|".repeat(tables.size())).append('\n');
            minutes.forEach(minute -> report.append(minute.row()).append('\n'));
            report.append('\n').append(summary(minutes)).append('\n');
        });
        Files.writeString(Path.of("target", "sustained-results.md"), report);
        log("%n%s", report);
    }

    /** The first five minutes against the last five: does anything drift? */
    private static String summary(List<Minute> minutes) {
        if (minutes.size() < 10) {
            return "Too short to compare the start with the end.";
        }
        List<Minute> first = minutes.subList(0, 5), last = minutes.subList(minutes.size() - 5, minutes.size());
        return String.format(Locale.ROOT, """
            First five minutes against the last five: completed %,.0f/s against %,.0f/s; claim p99 %.1f against %.1f ms; \
            completion p99 %.1f against %.1f ms; backlog %,d at the end; total size %,d MB against %,d MB.""",
            average(first, minute -> minute.completed() / 60.0), average(last, minute -> minute.completed() / 60.0),
            average(first, minute -> minute.claims() == null ? 0 : minute.claims().p99()), average(last, minute -> minute.claims() == null ? 0 : minute.claims().p99()),
            average(first, minute -> minute.completions() == null ? 0 : minute.completions().p99()),
            average(last, minute -> minute.completions() == null ? 0 : minute.completions().p99()),
            minutes.getLast().backlog(), size(minutes.getFirst()) / 1_000_000, size(minutes.getLast()) / 1_000_000);
    }

    private static double average(List<Minute> minutes, java.util.function.ToDoubleFunction<Minute> value) {
        return minutes.stream().mapToDouble(value).average().orElse(0);
    }

    private static long size(Minute minute) {
        return minute.tables().stream().mapToLong(table -> table.heapBytes() + table.indexBytes()).sum();
    }

    private static HikariDataSource pool(EmbeddedPostgres postgres, String database) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:" + postgres.getPort() + "/" + database);
        config.setUsername("postgres");
        config.setMaximumPoolSize(WORKERS + 4);
        return new HikariDataSource(config);
    }

    /** "60m", "2h", "90s" or an ISO duration. */
    private static Duration duration(String text) {
        String value = text.strip().toLowerCase(Locale.ROOT);
        if (value.startsWith("p")) {
            return Duration.parse(value.toUpperCase(Locale.ROOT));
        }
        long amount = Long.parseLong(value.substring(0, value.length() - 1));
        return switch (value.charAt(value.length() - 1)) {
            case 's' -> Duration.ofSeconds(amount);
            case 'm' -> Duration.ofMinutes(amount);
            case 'h' -> Duration.ofHours(amount);
            default -> throw new IllegalArgumentException("Not a duration: " + text + "; write it like 60m, 2h or 90s");
        };
    }

    private static void log(String format, Object... arguments) {
        System.out.printf(Locale.ROOT, format + "%n", arguments);
    }
}
