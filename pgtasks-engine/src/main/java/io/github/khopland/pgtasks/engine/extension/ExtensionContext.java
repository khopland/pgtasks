package io.github.khopland.pgtasks.engine.extension;

import java.time.InstantSource;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.github.khopland.pgtasks.TaskWriter;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskAdmin;
import io.github.khopland.pgtasks.engine.TaskObserver;
import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.jspecify.annotations.Nullable;

/**
 * What an {@link Extension} gets to work with in one engine: the engine's store, producer and
 * operator APIs, and the hooks it can register. Hooks are registered while the extension is
 * installed: one registered after {@link Extension#install} returns throws. The rest can be used
 * for as long as the engine lives, so an extension may keep its context.
 */
public interface ExtensionContext {

    /** The engine's store, for an extension's own statements. */
    TaskStore store();

    /**
     * The extension's own view of the store, from the first {@link io.github.khopland.pgtasks.spi.StoreCapability}
     * on the class path that provides one of this type for this store. One instance per store and
     * type: engines that share a store, as a {@code TaskTester} with several nodes does, share it.
     *
     * @throws IllegalStateException if none does: the module that provides it is missing
     */
    <C> C capability(Class<C> type);

    /** Pushes tasks, like any producer. */
    Tasks tasks();

    /**
     * Pushes in a transaction the extension opened on {@link #store()}, such as in
     * {@link #transact}, so the tasks commit or roll back with the extension's own writes in it:
     * a schedule that fires and the task it pushes, say. Payloads are encoded with the engine's
     * codecs, the observers see each push as it is written, and workers are woken once the
     * transaction commits. Nothing is committed here; the transaction's owner commits or rolls back.
     */
    TaskWriter tasks(StoreTransaction transaction);

    /** The operator's side, across the cluster: topics, tasks, dead letters and retention. */
    TaskAdmin admin();

    /** The store's clock: the database's on Postgres. */
    InstantSource clock();

    /** The topics this node registers, in the order they were registered, whether it runs them now or not. */
    List<TopicRef<?>> topics();

    /**
     * Runs work in a transaction of its own on {@link #store()}: committed if it returns, rolled
     * back if it throws. A failure comes out unchecked: as it was if it was unchecked, otherwise
     * as a {@link io.github.khopland.pgtasks.TaskStorageException} that says what failed.
     *
     * @param what what the work does, for the exception's message, such as "Reading the paused topics"
     */
    <R extends @Nullable Object> R transact(String what, Work<R> work);

    /** Work in a store transaction; see {@link #transact}. */
    @FunctionalInterface
    interface Work<R extends @Nullable Object> {
        R run(StoreTransaction transaction) throws Exception;
    }

    /**
     * A per-topic setting's value for a topic this node registers: what was set or overridden, or
     * the key's default. Empty for a topic this node doesn't register.
     */
    <T> Optional<T> topicSetting(TopicRef<?> topic, SettingKey<T> key);

    /**
     * An engine-wide setting's value: what the builder set or an override gave
     * ({@code extensions.<name>}), or the key's default.
     *
     * @throws IllegalArgumentException if no installed extension declares it in {@link Extension#engineSettings()}
     */
    <T> T setting(SettingKey<T> key);

    /**
     * Whether this node holds the lease of the {@linkplain HousekeepingJob#leader leader job} of
     * this name now. A leader job that loops over batches stops when it turns false. False for a
     * name this engine has no leader job by, and while the engine isn't running.
     */
    boolean leads(String job);

    /**
     * Runs this engine's job of that name once more, soon, besides its schedule: on its own thread
     * once a run in progress ends, as if its interval had passed. For work an operator asked for
     * now, such as a recurring task to run now. A leader job runs only on the node that holds its
     * lease; nothing runs while the engine isn't running.
     *
     * @throws IllegalArgumentException if this engine has no extension job by that name
     */
    void runSoon(String job);

    /**
     * Wakes this node's worker for a topic, so it asks the claim gates again now instead of after
     * the delay a gate gave: for a gate whose answer changed, such as on a message on its
     * {@link #channel}. Nothing for a topic this node doesn't run.
     */
    void wake(TopicRef<?> topic);

    /** Like {@link #wake(TopicRef)}, by the topic's name, such as one a message on a channel named. */
    void wake(String topic);

    /** Asked before every claim on this node; see {@link ClaimGate}. */
    void claimGate(ClaimGate gate);

    /** Told each batch's results on this node; see {@link BatchListener}. */
    void batchListener(BatchListener listener);

    /** Wraps every topic's handler on this node; see {@link HandlerDecorator}. */
    void handlerDecorator(HandlerDecorator decorator);

    /**
     * Sees this node's pushes and batches, next to the observers given to the builder, for
     * tracing, metrics or headers that follow the work; see {@link TaskObserver}. It comes after
     * them: for a header name two observers give, the builder's value is stored.
     */
    void observer(TaskObserver observer);

    /**
     * Runs a job on a fixed delay while the engine runs; see {@link HousekeepingJob}.
     *
     * @throws IllegalArgumentException if this engine has a job by that name already, its own
     *                                  included, such as {@code promotion}
     */
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

    /**
     * Like {@link #channel(String, Runnable)}, told what the messages since the last run said:
     * their payloads, or an empty set when anything may have changed, after a lost connection,
     * on the heartbeat, or when more messages came in than it is worth listing.
     */
    void channel(String name, Reload reload);

    /** A reload told what changed; see {@link #channel(String, Reload)}. */
    @FunctionalInterface
    interface Reload {
        /** @param payloads the messages' payloads, or empty when anything may have changed */
        void reload(Set<String> payloads) throws Exception;
    }

    /**
     * Shows operators what the extension knows and lets them act on it, through the starter's
     * endpoint and dashboard; see {@link OperatorView}.
     *
     * @throws IllegalArgumentException if this engine has a view by that name already
     */
    void operatorView(OperatorView view);

    /**
     * Runs each time the engine starts, before its topics and jobs do, so nothing is claimed
     * before the extension is ready; also after a restart. One that throws fails the start.
     */
    void onStart(Runnable action);

    /** Runs each time the engine stops, before its topics stop. One that throws is logged. */
    void onStop(Runnable action);
}
