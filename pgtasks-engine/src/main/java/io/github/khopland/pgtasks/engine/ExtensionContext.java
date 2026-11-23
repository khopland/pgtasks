package io.github.khopland.pgtasks.engine;

import java.time.InstantSource;

import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.spi.TaskStore;

/**
 * What an {@link Extension} gets to work with: the engine's store and producer API, and the
 * hooks it can register. Hooks are registered while the extension is installed; the engine calls
 * them while it runs.
 */
public interface ExtensionContext {

    /** The engine's store, for an extension's own statements. */
    TaskStore store();

    /** Pushes tasks, like any producer. */
    Tasks tasks();

    /** The store's clock: the database's on Postgres. */
    InstantSource clock();

    /** Whether this node holds the housekeeping lease now, so it runs the cluster-wide jobs. */
    boolean leading();

    /** Runs a job on a fixed delay while the engine runs. */
    void job(HousekeepingJob job);

    /**
     * Reloads something when it may have changed on another node: on a message on the channel
     * (sent with {@link TaskStore#notify} in the transaction that changed it, so it arrives once
     * that commits), after the notification connection was lost, and on every heartbeat, since a
     * message may get lost. Runs on a thread of its own; requests that arrive while one waits are
     * served by that one run. Channel names are lowercase words ({@link io.github.khopland.pgtasks.spi.Notifier#CHANNEL});
     * {@code ready} and {@code topic} are the engine's.
     */
    void channel(String name, Runnable reload);

    /** Runs each time the engine starts, after its topics run; also after a restart. */
    void onStart(Runnable action);

    /** Runs each time the engine stops, before its topics stop. */
    void onStop(Runnable action);
}
