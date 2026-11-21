package io.github.khopland.task2.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Samples what the benchmark's backends are doing every few milliseconds: which statement
 * (claim or completion) and what it waits on, or whether it is on CPU, plus any autovacuum
 * worker and the table it works on. The summary says where the time of a scenario went.
 */
final class WaitSampler implements AutoCloseable {

    private static final String SAMPLE = """
        SELECT CASE
                   WHEN backend_type = 'autovacuum worker' THEN coalesce(substring(query from 'autovacuum: (.*)'), 'autovacuum')
                   WHEN query LIKE '%picked%' THEN 'claim'
                   WHEN query LIKE '%DELETE FROM tasks.queue%' OR query LIKE '%SET state = ''succeeded''%' THEN 'complete'
                   ELSE left(regexp_replace(query, '\\s+', ' ', 'g'), 50)
               END
               || ' | ' || coalesce(wait_event_type || ':' || wait_event, 'CPU')
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND (backend_type = 'autovacuum worker' OR (datname = current_database() AND backend_type = 'client backend' AND state = 'active'))
        """;

    private final Connection connection;
    private final Thread thread;
    private final Map<String, Integer> counts = new HashMap<>();
    private volatile boolean running = true;
    private int samples;

    private WaitSampler(String url) throws SQLException {
        connection = DriverManager.getConnection(url, "postgres", null);
        thread = Thread.ofPlatform().name("wait-sampler").daemon().start(this::run);
    }

    static WaitSampler start(String url) throws SQLException {
        return new WaitSampler(url);
    }

    private void run() {
        try (Statement statement = connection.createStatement()) {
            while (running) {
                try (ResultSet resultSet = statement.executeQuery(SAMPLE)) {
                    synchronized (counts) {
                        samples++;
                        while (resultSet.next()) {
                            counts.merge(resultSet.getString(1), 1, Integer::sum);
                        }
                    }
                }
                Thread.sleep(5);
            }
        } catch (SQLException | InterruptedException e) {
            if (running) {
                System.out.println("wait sampler stopped: " + e);
            }
        }
    }

    /** One line per state, most frequent first, as the average number of backends in it. */
    List<String> summary() {
        synchronized (counts) {
            int total = Math.max(samples, 1);
            return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .map(entry -> String.format(Locale.ROOT, "%5.2f  %s", entry.getValue() / (double) total, entry.getKey()))
                .collect(Collectors.toList());
        }
    }

    @Override
    public void close() throws Exception {
        running = false;
        thread.join(1_000);
        connection.close();
    }
}
