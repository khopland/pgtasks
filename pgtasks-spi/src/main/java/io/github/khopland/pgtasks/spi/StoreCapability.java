package io.github.khopland.pgtasks.spi;

import java.util.Optional;

/**
 * Gives an extension what it needs from a particular store, such as its own statements on
 * Postgres and its own maps in memory, without the core {@link TaskStore} knowing about it. An
 * extension module ships an interface, say {@code WindowStore}, and a provider per store it
 * supports, registered for {@link java.util.ServiceLoader} in
 * {@code META-INF/services/io.github.khopland.pgtasks.spi.StoreCapability}. The extension asks
 * for it with {@code ExtensionContext.capability(WindowStore.class)}.
 */
public interface StoreCapability {

    /**
     * The capability for this store, or empty if this provider doesn't provide that type for it.
     * Asked once per extension that wants it.
     */
    <C> Optional<C> create(Class<C> type, TaskStore store);
}
