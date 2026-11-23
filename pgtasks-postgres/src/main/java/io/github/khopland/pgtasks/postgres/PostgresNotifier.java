package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.khopland.pgtasks.spi.Notifier;

import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LISTENs for the notifications that the queue triggers send when tasks become ready, and that
 * changes to topics send. Uses one
 * long-lived connection; on any error it reconnects with backoff and reports
 * {@link Listener#missed()}, so the engine polls every topic once.
 *
 * <p>Waiting for notifications doesn't notice a connection the network dropped without closing
 * (a failover, a NAT or firewall timing out an idle flow): the socket just stays quiet. So every
 * 30 seconds the notifier runs a query on it, with a network timeout, and
 * reconnects when that fails.
 */
public final class PostgresNotifier implements Notifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresNotifier.class);
    private static final int LIVENESS_SECONDS = 30;
    private static final Duration LIVENESS_TIMEOUT = Duration.ofSeconds(10);

    /** Opens the listening connection. It should not come from the application's pool. */
    @FunctionalInterface
    public interface ConnectionFactory {
        Connection open() throws SQLException;
    }

    private final ConnectionFactory connections;
    // The schema's channels, so two schemas in one database don't wake each other's nodes.
    private final String schema;
    private final String ready;
    private final Duration livenessInterval;
    private final Duration livenessTimeout;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile @Nullable Thread thread;
    private volatile boolean connected;

    /** Listens to the {@value TaskSchema#DEFAULT_SCHEMA} schema's channels. */
    public PostgresNotifier(ConnectionFactory connections) {
        this(connections, TaskSchema.DEFAULT_SCHEMA);
    }

    /** Listens to the given schema's channels: {@code <schema>_ready}, and {@code <schema>_<channel>} for each channel it is started with. */
    public PostgresNotifier(ConnectionFactory connections, String schema) {
        this(connections, schema, Duration.ofSeconds(LIVENESS_SECONDS), LIVENESS_TIMEOUT);
    }

    /** For tests: how often the connection is checked, and how long a check may take. */
    PostgresNotifier(ConnectionFactory connections, String schema, Duration livenessInterval, Duration livenessTimeout) {
        this.livenessInterval = Objects.requireNonNull(livenessInterval);
        this.livenessTimeout = Objects.requireNonNull(livenessTimeout);
        this.connections = Objects.requireNonNull(connections);
        TaskSchema.validName(schema);
        this.schema = schema;
        this.ready = schema + "_ready";
    }

    @Override
    public void start(Listener listener, Set<String> channels) {
        Map<String, String> byName = new HashMap<>();
        for (String channel : channels) {
            if (!CHANNEL.matcher(channel).matches() || channel.equals("ready")) {
                throw new IllegalArgumentException("Invalid channel '" + channel + "': a lowercase word other than ready");
            }
            byName.put(schema + "_" + channel, channel);
        }
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Already started");
        }
        thread = Thread.ofVirtual().name("pgtasks-notifier").start(() -> listen(listener, Map.copyOf(byName)));
    }

    /** @param channels the channels to listen to, by their name in the database */
    private void listen(Listener listener, Map<String, String> channels) {
        Duration backoff = Duration.ofMillis(100);
        boolean first = true;
        while (running.get()) {
            try (Connection connection = connections.open()) {
                // A pool may hand out connections with auto-commit off; LISTEN only takes effect at
                // commit, so the session would sit idle in transaction and hear nothing.
                connection.setAutoCommit(true);
                // Bounds the liveness check's wait for a reply; waiting for notifications sets its own.
                connection.setNetworkTimeout(Runnable::run, Math.toIntExact(livenessTimeout.toMillis()));
                try (Statement statement = connection.createStatement()) {
                    statement.execute("LISTEN " + ready);
                    for (String channel : channels.keySet()) {
                        statement.execute("LISTEN " + channel);
                    }
                }
                connected = true;
                if (!first) {
                    listener.missed();
                }
                first = false;
                backoff = Duration.ofMillis(100);
                PGConnection postgres = connection.unwrap(PGConnection.class);
                long checked = System.nanoTime();
                while (running.get()) {
                    if (System.nanoTime() - checked >= livenessInterval.toNanos()) {
                        try (Statement statement = connection.createStatement()) {
                            statement.execute("SELECT 1");
                        }
                        checked = System.nanoTime();
                    }
                    PGNotification[] notifications = postgres.getNotifications(500);
                    if (notifications == null) {
                        continue;
                    }
                    for (PGNotification notification : notifications) {
                        String channel = channels.get(notification.getName());
                        if (channel != null) {
                            listener.notified(channel, notification.getParameter());
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
                LOGGER.warn("Lost the notification connection, reconnecting in {}", backoff, e);
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
