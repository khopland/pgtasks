package org.example.task2.spi;

import java.util.Map;

import javax.sql.DataSource;

/**
 * Creates a task store for a DataSource. Found with {@link java.util.ServiceLoader}, so the engine's
 * builder can turn {@code .dataSource(ds)} into the Postgres store without depending on it. The
 * in-memory store has no provider: it is only ever chosen explicitly.
 */
public interface StoreProvider {

    /** Lower runs first; the first provider that supports the DataSource wins. */
    default int order() {
        return 0;
    }

    boolean supports(DataSource dataSource);

    /** @param options store options, such as {@code schema-mode=validate} */
    TaskStore create(DataSource dataSource, Map<String, String> options);

    /** Wake-ups for this store, or {@code null} to rely on polling. */
    default Notifier notifier(DataSource dataSource) {
        return null;
    }
}
