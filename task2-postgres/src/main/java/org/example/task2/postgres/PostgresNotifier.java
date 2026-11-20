package org.example.task2.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.example.task2.spi.Notifier;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * LISTENs for the notifications that the queue triggers send when tasks become ready, and that
 * changes to rate limit overrides send. Uses one
 * long-lived connection; on any error it reconnects with backoff and reports
 * {@link Listener#missed()}, so the engine polls every topic once.
 */
public final class PostgresNotifier implements Notifier {

    private static final System.Logger LOGGER = System.getLogger(PostgresNotifier.class.getName());

    /** Opens the listening connection. It should not come from the application's pool. */
    @FunctionalInterface
    public interface ConnectionFactory {
        Connection open() throws SQLException;
    }

    private static final String READY = "tasks_ready";
    private static final String LIMIT = "tasks_limit";

    private final ConnectionFactory connections;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Thread thread;

    public PostgresNotifier(ConnectionFactory connections) {
        this.connections = Objects.requireNonNull(connections);
    }

    @Override
    public void start(Listener listener) {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Already started");
        }
        thread = Thread.ofVirtual().name("task2-notifier").start(() -> listen(listener));
    }

    private void listen(Listener listener) {
        Duration backoff = Duration.ofMillis(100);
        boolean first = true;
        while (running.get()) {
            try (Connection connection = connections.open()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("LISTEN " + READY);
                    statement.execute("LISTEN " + LIMIT);
                }
                if (!first) {
                    listener.missed();
                }
                first = false;
                backoff = Duration.ofMillis(100);
                PGConnection postgres = connection.unwrap(PGConnection.class);
                while (running.get()) {
                    PGNotification[] notifications = postgres.getNotifications(500);
                    if (notifications == null) {
                        continue;
                    }
                    for (PGNotification notification : notifications) {
                        if (LIMIT.equals(notification.getName())) {
                            listener.limitChanged(notification.getParameter());
                            continue;
                        }
                        try {
                            listener.ready(Integer.parseInt(notification.getParameter()));
                        } catch (NumberFormatException ignored) {
                            listener.missed();
                        }
                    }
                }
            } catch (SQLException | RuntimeException e) {
                if (!running.get()) {
                    return;
                }
                LOGGER.log(System.Logger.Level.WARNING, "Lost the notification connection, reconnecting in " + backoff, e);
                first = false;
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException interrupted) {
                    return;
                }
                backoff = backoff.multipliedBy(2).compareTo(Duration.ofSeconds(30)) > 0 ? Duration.ofSeconds(30) : backoff.multipliedBy(2);
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        Thread current = thread;
        if (current != null) {
            current.interrupt();
            try {
                current.join(Duration.ofSeconds(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
