package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The notifier's long-lived connection. Taken from the application's pool, it would hold one of
 * the pool's connections for as long as the engine runs, so it is opened directly when the pool
 * says how to connect (HikariCP does). If that is refused for a reason other than the database
 * being unreachable, such as credentials the pool gets some other way, it falls back to the pool.
 */
final class NotifierConnections implements PostgresNotifier.ConnectionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotifierConnections.class);

    private final DataSource dataSource;
    private final PoolInfo.@Nullable Direct direct;
    private volatile boolean fromPool;

    private NotifierConnections(DataSource dataSource, PoolInfo.@Nullable Direct direct) {
        this.dataSource = dataSource;
        this.direct = direct;
        this.fromPool = direct == null;
    }

    static NotifierConnections of(DataSource dataSource) {
        PoolInfo.Direct direct = PoolInfo.direct(dataSource).orElse(null);
        if (direct == null && !PoolInfo.unpooled(dataSource)) {
            LOGGER.info("Notifications listen on a connection from {}, which holds one of its connections while the engine runs; size the "
                + "pool for it", dataSource.getClass().getSimpleName());
        }
        return new NotifierConnections(dataSource, direct);
    }

    /** Whether the next connection comes from the pool. */
    boolean fromPool() {
        return fromPool;
    }

    @Override
    public Connection open() throws SQLException {
        PoolInfo.Direct target = direct;
        if (!fromPool && target != null) {
            try {
                return DriverManager.getConnection(target.url(), target.properties());
            } catch (SQLException e) {
                if (e.getSQLState() != null && e.getSQLState().startsWith("08")) {
                    throw e; // unreachable: the notifier retries
                }
                fromPool = true;
                LOGGER.info("Could not open the notification connection outside the pool ({}); it holds one of the pool's connections "
                    + "instead", e.getMessage());
            }
        }
        return dataSource.getConnection();
    }
}
