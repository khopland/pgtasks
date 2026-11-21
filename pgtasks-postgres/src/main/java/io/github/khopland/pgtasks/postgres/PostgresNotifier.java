package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.khopland.pgtasks.spi.Notifier;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import org.jspecify.annotations.Nullable;

/**
 * LISTENs for the notifications that the queue triggers send when tasks become ready, and that
 * changes to topics and rate limit overrides send. Uses one
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
    private static final String TOPIC = "tasks_topic";

    private final ConnectionFactory connections;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile @Nullable Thread thread;
    private volatile boolean connected;

    public PostgresNotifier(ConnectionFactory connections) {
        this.connections = Objects.requireNonNull(connections);
    }

    @Override
    public void start(Listener listener) {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Already started");
        }
        thread = Thread.ofVirtual().name("pgtasks-notifier").start(() -> listen(listener));
    }

    private void listen(Listener listener) {
        Duration backoff = Duration.ofMillis(100);
        boolean first = true;
        while (running.get()) {
            try (Connection connection = connections.open()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("LISTEN " + READY);
                    statement.execute("LISTEN " + LIMIT);
                    statement.execute("LISTEN " + TOPIC);
                }
                connected = true;
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
                        if (TOPIC.equals(notification.getName())) {
                            listener.topicChanged(notification.getParameter());
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
                connected = false;
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
    public boolean connected() {
        return connected && running.get();
    }

    @Override
    public void close() {
        running.set(false);
        Thread current = thread;
        if (current != null) {
            try {
                // The listener checks running at least every 500 ms. Interrupting a virtual thread
                // blocked in a socket read closes the socket, which a pool then reports as a broken
                // connection, so an interrupt is the last resort.
                current.join(Duration.ofSeconds(2));
                if (current.isAlive()) {
                    current.interrupt();
                    current.join(Duration.ofSeconds(3));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
