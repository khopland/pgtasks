package io.github.khopland.pgtasks.benchmark;

import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.Work;
import io.github.khopland.pgtasks.engine.Retry;
import io.github.khopland.pgtasks.engine.TaskEngine;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A long run of the real engine through its public API, under steady load and repeated trouble,
 * to show that nothing is lost, nothing runs its side effects twice, and nothing grows without
 * bound. Several nodes, each with its own pool, run a work topic whose handler pushes one child
 * task in its completion, sleeps a little and fails 1 % of the time. A producer pushes at a
 * steady rate through whichever nodes are up. Every {@code benchmark.soak.chaos-every} one of
 * these happens, in turn:
 * <ul>
 *   <li><b>kill</b>: a node's network is cut and its backends terminated, as with {@code kill -9};
 *       a replacement starts once the others have taken over its claims;</li>
 *   <li><b>freeze</b>: a node stops getting connections for twice the expiry, like a long GC
 *       pause, then carries on: its stale completions must be fenced;</li>
 *   <li><b>backends</b>: every node's backends are terminated at once, as in a Postgres restart;</li>
 *   <li><b>outage</b>: the database is unreachable from every node for 15 s;</li>
 *   <li><b>rolling</b>: every node is stopped and replaced, one at a time, as in a deploy.</li>
 * </ul>
 * Every minute it records throughput, the backlog, pickup latency, re-runs, table sizes, dead
 * rows, connections, heap after GC and threads, to {@code target/soak-results.md}, written as it
 * goes so a run that dies still leaves its numbers. At the end the producer stops, the queue
 * drains, and it checks: every work task succeeded, each has exactly one committed child, and
 * nothing is left in the queue or the dead letters.
 * <pre>
 * mvn -Pbenchmark -pl pgtasks-benchmark -am test -Dtest=SoakBenchmark -Dsurefire.failIfNoSpecifiedTests=false -Dbenchmark.soak=24h
 * </pre>
 * System properties: {@code benchmark.soak} (how long to load; without it the test is skipped),
 * {@code benchmark.soak.rate} (work tasks a second, default 50), {@code benchmark.soak.nodes}
 * (default 3), {@code benchmark.soak.chaos-every} (default 10m), {@code benchmark.soak.drain}
 * (how long the queue may take to drain at the end, default 15m) and
 * {@code benchmark.data-directory} (default a temporary directory).
 */
@EnabledIfSystemProperty(named = "benchmark.soak", matches = ".+")
class SoakBenchmark {

    private static final Duration LENGTH = duration(System.getProperty("benchmark.soak", "10m"));
    private static final int RATE = Integer.getInteger("benchmark.soak.rate", 50);
    private static final int NODES = Integer.getInteger("benchmark.soak.nodes", 3);
    private static final Duration CHAOS_EVERY = duration(System.getProperty("benchmark.soak.chaos-every", "10m"));
    private static final Duration DRAIN = duration(System.getProperty("benchmark.soak.drain", "15m"));
    private static final Duration HEARTBEAT = Duration.ofSeconds(1);
    private static final Duration EXPIRY = Duration.ofSeconds(10);
    private static final Duration MINUTE = Duration.ofMinutes(1);
    private static final double FAILURE_RATE = 0.01;

    private static final TopicRef<String> WORK = TopicRef.of("soak.work", String.class);
    private static final TopicRef<String> CHILD = TopicRef.of("soak.child", String.class);
    private static final List<String> EVENTS = List.of("kill", "freeze", "backends", "outage", "rolling");

    private final List<Node> nodes = new CopyOnWriteArrayList<>();
    private final AtomicInteger generation = new AtomicInteger();
    private final AtomicLong pushed = new AtomicLong();
    private final AtomicLong pushFailures = new AtomicLong();
    private final AtomicLong injectedFailures = new AtomicLong();
    private final Latencies latencies = new Latencies();
    private final List<String> eventsThisMinute = new CopyOnWriteArrayList<>();
    private volatile boolean producing = true;
    private String jdbcUrl = "";
    private HikariDataSource admin;

    @Test
    void the_engine_survives_a_long_run_with_trouble() throws Exception {
        Path target = Path.of("target");
        Files.createDirectories(target);
        EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder()
            .setServerConfig("fsync", "on")
            .setServerConfig("synchronous_commit", "on")
            .setServerConfig("full_page_writes", "on")
            .setServerConfig("max_connections", "300")
            .setServerConfig("shared_buffers", System.getProperty("benchmark.shared-buffers", "512MB"))
            .setServerConfig("max_wal_size", "4GB");
        String directory = System.getProperty("benchmark.data-directory");
        if (directory != null) {
            builder.setDataDirectory(Path.of(directory)).setCleanDataDirectory(true);
        }
        try (EmbeddedPostgres postgres = builder.start();
             PrintWriter results = new PrintWriter(Files.newBufferedWriter(target.resolve("soak-results.md")), true)) {
            jdbcUrl = postgres.getJdbcUrl("postgres", "postgres");
            admin = pool("soak-admin", 4);
            run(results);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    private void run(PrintWriter results) throws Exception {
        results.printf(Locale.ROOT, "# Soak run%n%nStarted %s. %s of load at %d work tasks/s on %d nodes, trouble every %s (%s in turn), "
                + "heartbeat %s, expiry %s, %.0f %% of runs fail. Java %s.%n%n",
            Instant.now(), LENGTH, RATE, NODES, CHAOS_EVERY, String.join(", ", EVENTS), HEARTBEAT, EXPIRY, FAILURE_RATE * 100,
            Runtime.version());
        results.println("| min | work pushed | work done | backlog | dead | re-runs | pickup p50 ms | p99 ms | max ms | task MB | queue MB "
            + "| queue dead rows | connections | heap MB | threads | events |");
        results.println("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|");
        for (int index = 0; index < NODES; index++) {
            nodes.add(startNode());
        }
        // Platform threads: on JDK 25 a virtual thread's timed sleep can wake seconds late (JDK-8370887),
        // which would make the load and the trouble less than they say.
        Thread producer = Thread.ofPlatform().name("soak-producer").start(this::produce);
        Thread chaos = Thread.ofPlatform().name("soak-chaos").start(this::chaos);

        long started = System.nanoTime();
        long lastDone = 0;
        int minute = 0;
        while (System.nanoTime() - started < LENGTH.toNanos()) {
            Thread.sleep(Math.min(MINUTE.toMillis(), Math.max(1, (LENGTH.toNanos() - (System.nanoTime() - started)) / 1_000_000)));
            minute++;
            lastDone = sample(results, minute, lastDone);
        }
        producing = false;
        chaos.interrupt();
        producer.join();
        chaos.join();

        // Drain with every node healthy, then stop them as a deploy would.
        nodes.forEach(node -> node.pool.restore());
        long drainStarted = System.nanoTime();
        while (count("SELECT count(*) FROM pgtasks.queue WHERE state <> 'dead_letter'") > 0 && System.nanoTime() - drainStarted < DRAIN.toNanos()) {
            Thread.sleep(1_000);
        }
        Duration drained = Duration.ofNanos(System.nanoTime() - drainStarted);
        sample(results, minute + 1, lastDone);
        for (Node node : nodes) {
            node.engine.stop(Duration.ofSeconds(30));
            node.pool.close();
        }

        long work = count("SELECT count(*) FROM pgtasks.task WHERE topic_id = " + topicId(WORK));
        long succeeded = count("SELECT count(*) FROM pgtasks.task WHERE topic_id = " + topicId(WORK) + " AND outcome = 'succeeded'");
        long children = count("SELECT count(*) FROM pgtasks.task WHERE topic_id = " + topicId(CHILD));
        long parents = count("SELECT count(DISTINCT parent_id) FROM pgtasks.task WHERE topic_id = " + topicId(CHILD));
        long left = count("SELECT count(*) FROM pgtasks.queue");
        long dead = count("SELECT count(*) FROM pgtasks.queue WHERE state = 'dead_letter'");
        long reruns = count("SELECT count(*) FROM pgtasks.task WHERE topic_id = " + topicId(WORK) + " AND attempts > 1");
        long expired = count("SELECT count(*) FROM pgtasks.task WHERE topic_id = " + topicId(WORK) + " AND error::text LIKE '%xpired%'");
        results.printf(Locale.ROOT, "%n## Result%n%n- Drained in %d s after the load stopped.%n"
                + "- Work tasks: %,d pushed (%,d pushes refused while nodes were down), %,d succeeded.%n"
                + "- Children: %,d, from %,d distinct parents.%n"
                + "- Left in the queue: %,d, of which dead letters: %,d.%n"
                + "- Work tasks that ran more than once: %,d (injected failures: %,d; claims expired with their node: %,d).%n",
            drained.toSeconds(), work, pushFailures.get(), succeeded, children, parents, left, dead, reruns, injectedFailures.get(), expired);

        assertThat(left).as("tasks left in the queue after the drain").isZero();
        assertThat(succeeded).as("succeeded work tasks").isEqualTo(work);
        assertThat(children).as("children: exactly one per work task, however often it ran").isEqualTo(work);
        assertThat(parents).as("work tasks with a child").isEqualTo(work);
        results.println("- Every check passed: nothing lost, nothing completed twice.");
    }

    // --- nodes --------------------------------------------------------------------------------

    private record Node(String name, ChaosPool pool, TaskEngine engine) {
    }

    private Node startNode() {
        String name = "soak-node-" + generation.incrementAndGet();
        ChaosPool pool = new ChaosPool(pool(name, 12), name);
        TaskEngine engine = TaskEngine.builder()
            .dataSource(pool)
            .node(name)
            .heartbeat(HEARTBEAT, EXPIRY)
            // A task can lose its node many times in a long run with this much trouble; don't dead-letter it for that.
            .globals(globals -> globals.housekeeping(housekeeping -> housekeeping.expiries(1_000, Duration.ofSeconds(1))))
            .configure(tasks -> tasks
                .eachTask(WORK, this::work, options -> options.batchSize(20).concurrency(4)
                    .retry(Retry.exponential(Duration.ofMillis(200), Duration.ofSeconds(5)).forever()))
                .eachTask(CHILD, task -> { }, options -> options.batchSize(100).concurrency(2)))
            .build()
            .start();
        return new Node(name, pool, engine);
    }

    private void work(Work<String> task) throws InterruptedException {
        latencies.add(Duration.between(task.task().createdAt(), Instant.now()).toMillis());
        Thread.sleep(ThreadLocalRandom.current().nextInt(1, 10));
        if (ThreadLocalRandom.current().nextDouble() < FAILURE_RATE) {
            injectedFailures.incrementAndGet();
            throw new IllegalStateException("Injected failure");
        }
        // Committed with the completion: one child per work task, however often the work runs.
        task.push(CHILD, task.identifier(), "child");
    }

    private void produce() {
        long sequence = 0;
        int perTick = Math.max(1, RATE / 10);
        while (producing) {
            long tick = System.nanoTime();
            List<Node> up = nodes.stream().filter(node -> node.pool.healthy()).toList();
            if (!up.isEmpty()) {
                Node node = up.get(ThreadLocalRandom.current().nextInt(up.size()));
                List<Creation<String>> batch = new ArrayList<>(perTick);
                for (int index = 0; index < perTick; index++) {
                    batch.add(Creation.of("w-" + sequence++, "work"));
                }
                try {
                    node.engine.tasks().independently().push(WORK, Insertion.APPEND, batch);
                    pushed.addAndGet(batch.size());
                } catch (RuntimeException e) {
                    // Counted by what the database holds at the end, so a push whose commit was lost doesn't matter.
                    pushFailures.addAndGet(batch.size());
                }
            }
            sleepNanos(Duration.ofMillis(100).toNanos() - (System.nanoTime() - tick));
        }
    }

    // --- trouble ------------------------------------------------------------------------------

    private void chaos() {
        int turn = 0;
        try {
            while (producing) {
                Thread.sleep(CHAOS_EVERY);
                String event = EVENTS.get(turn++ % EVENTS.size());
                eventsThisMinute.add(event);
                switch (event) {
                    case "kill" -> kill(nodes.getFirst());
                    case "freeze" -> freeze(nodes.get(ThreadLocalRandom.current().nextInt(nodes.size())));
                    case "backends" -> nodes.forEach(node -> node.pool.terminate());
                    case "outage" -> {
                        nodes.forEach(node -> node.pool.cut());
                        Thread.sleep(15_000);
                        nodes.forEach(node -> node.pool.restore());
                    }
                    case "rolling" -> {
                        for (Node node : List.copyOf(nodes)) {
                            nodes.remove(node);
                            node.engine.stop(Duration.ofSeconds(30));
                            node.pool.close();
                            nodes.add(startNode());
                            Thread.sleep(10_000);
                        }
                    }
                    default -> throw new IllegalStateException(event);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Gone without a word; once the others have taken over its claims, a replacement starts. */
    private void kill(Node node) throws InterruptedException {
        nodes.remove(node);
        node.pool.cut();
        Thread.ofPlatform().name("soak-reap-" + node.name).start(() -> {
            // The dead process's threads are what stop() ends; it can't reach the database to hand anything back.
            node.engine.stop(Duration.ZERO);
            node.pool.close();
        });
        Thread.sleep(EXPIRY.multipliedBy(2));
        nodes.add(startNode());
    }

    /** Paused past its expiry, then back: whatever it completes from before the pause must be refused. */
    private void freeze(Node node) throws InterruptedException {
        node.pool.freeze();
        Thread.sleep(EXPIRY.multipliedBy(2));
        node.pool.restore();
    }

    // --- measuring ----------------------------------------------------------------------------

    private long sample(PrintWriter results, int minute, long lastDone) throws SQLException {
        int work = topicId(WORK);
        long done = count("SELECT count(*) FROM pgtasks.task WHERE topic_id = " + work + " AND outcome = 'succeeded'");
        long total = count("SELECT count(*) FROM pgtasks.task WHERE topic_id = " + work);
        long backlog = count("SELECT count(*) FROM pgtasks.queue");
        long dead = count("SELECT count(*) FROM pgtasks.queue WHERE state = 'dead_letter'");
        long reruns = count("SELECT count(*) FROM pgtasks.task WHERE topic_id = " + work + " AND attempts > 1");
        long taskBytes = count("SELECT pg_total_relation_size('pgtasks.task')");
        long queueBytes = count("SELECT pg_total_relation_size('pgtasks.queue')");
        long queueDead = count("SELECT coalesce(n_dead_tup, 0) FROM pg_stat_user_tables WHERE schemaname = 'pgtasks' AND relname = 'queue'");
        long connections = count("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'soak-node-%'");
        long[] pickup = latencies.drain();
        List<String> events = new ArrayList<>(eventsThisMinute);
        eventsThisMinute.clear();
        results.printf(Locale.ROOT, "| %d | %,d | %,d | %,d | %,d | %,d | %d | %d | %d | %.0f | %.1f | %,d | %d | %.0f | %d | %s |%n",
            minute, total, done - lastDone, backlog, dead, reruns, percentile(pickup, 0.50), percentile(pickup, 0.99),
            pickup.length == 0 ? 0 : pickup[pickup.length - 1], taskBytes / 1e6, queueBytes / 1e6, queueDead, connections,
            heapAfterGc() / 1e6, ManagementFactory.getThreadMXBean().getThreadCount(), String.join(", ", events));
        return done;
    }

    /** Heap still in use after the last collection of each pool: what the process keeps, not what it churns. */
    private static long heapAfterGc() {
        long used = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP && pool.getCollectionUsage() != null) {
                used += pool.getCollectionUsage().getUsed();
            }
        }
        return used;
    }

    private static long percentile(long[] sorted, double fraction) {
        return sorted.length == 0 ? 0 : sorted[Math.min(sorted.length - 1, (int) Math.ceil(fraction * sorted.length) - 1)];
    }

    /** Pickup latencies of one minute: from the push to the handler starting, in milliseconds. */
    private static final class Latencies {
        private long[] values = new long[1 << 16];
        private int size;

        synchronized void add(long millis) {
            if (size == values.length) {
                values = Arrays.copyOf(values, size * 2);
            }
            values[size++] = millis;
        }

        synchronized long[] drain() {
            long[] taken = Arrays.copyOf(values, size);
            size = 0;
            Arrays.sort(taken);
            return taken;
        }
    }

    private int topicId(TopicRef<?> topic) throws SQLException {
        return (int) count("SELECT coalesce((SELECT id FROM pgtasks.topic WHERE name = '" + topic.name() + "'), 0)");
    }

    private long count(String sql) throws SQLException {
        try (Connection connection = admin.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private HikariDataSource pool(String name, int size) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setMaximumPoolSize(size);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(2_000);
        config.setPoolName(name);
        config.addDataSourceProperty("ApplicationName", name);
        return new HikariDataSource(config);
    }

    private static void sleepNanos(long nanos) {
        if (nanos > 0) {
            try {
                Thread.sleep(Duration.ofNanos(nanos));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Duration duration(String text) {
        String value = text.strip().toLowerCase(Locale.ROOT);
        long amount = Long.parseLong(value.substring(0, value.length() - 1));
        return switch (value.charAt(value.length() - 1)) {
            case 's' -> Duration.ofSeconds(amount);
            case 'm' -> Duration.ofMinutes(amount);
            case 'h' -> Duration.ofHours(amount);
            default -> throw new IllegalArgumentException("Not a duration: " + text + "; write it like 90s, 60m or 24h");
        };
    }

    /**
     * One node's pool with switches for trouble. Cut refuses connections and terminates the
     * node's backends, like a partition or a dead process; frozen blocks new connections, like a
     * paused one.
     */
    private final class ChaosPool implements DataSource, AutoCloseable {
        private enum Mode { NORMAL, CUT, FROZEN }

        private final HikariDataSource pool;
        private final String name;
        private final Object gate = new Object();
        private volatile Mode mode = Mode.NORMAL;

        ChaosPool(HikariDataSource pool, String name) {
            this.pool = pool;
            this.name = name;
        }

        boolean healthy() {
            return mode == Mode.NORMAL && !pool.isClosed();
        }

        @Override
        public Connection getConnection() throws SQLException {
            while (true) {
                switch (mode) {
                    case NORMAL -> {
                        return pool.getConnection();
                    }
                    case CUT -> throw new SQLTransientConnectionException("The network to the database is cut for " + name);
                    case FROZEN -> {
                        synchronized (gate) {
                            while (mode == Mode.FROZEN) {
                                try {
                                    gate.wait();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new SQLTransientConnectionException("Interrupted while frozen", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        void cut() {
            mode = Mode.CUT;
            terminate();
        }

        void freeze() {
            mode = Mode.FROZEN;
        }

        void restore() {
            synchronized (gate) {
                mode = Mode.NORMAL;
                gate.notifyAll();
            }
        }

        void terminate() {
            try (Connection connection = admin.getConnection();
                 PreparedStatement statement = connection.prepareStatement(
                     "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = ?")) {
                statement.setString(1, name);
                statement.executeQuery().close();
            } catch (SQLException e) {
                throw new IllegalStateException("Could not terminate the backends of " + name, e);
            }
        }

        @Override
        public void close() {
            restore();
            pool.close();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public <T> T unwrap(Class<T> type) throws SQLException {
            throw new SQLException("Not a wrapper");
        }

        @Override
        public boolean isWrapperFor(Class<?> type) {
            return false;
        }
    }
}
