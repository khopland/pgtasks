package io.github.khopland.pgtasks.engine.extension;

import java.sql.SQLException;
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
 *
 * <p>The engine starts an extension's hooks in this order: its {@linkplain #onStart start
 * actions}, then its {@linkplain #channel channel reloads} (messages that arrived meanwhile are
 * kept for them), its {@linkplain #job jobs}, and the topics, which ask its gates. It stops them
 * the other way round: the topics finish their batches, the reloads and jobs end, and then the
 * {@linkplain #onStop stop actions} run.
 *
 * <p>The engine implements this interface, its {@link Job} and its {@link Channel}; extensions
 * only call them. Minor releases add methods to them, so don't implement them.
 */
public interface ExtensionContext {

    /** The engine's store, for an extension's own statements. */
    TaskStore store();

    /**
     * The extension's own view of the store, from the first {@link io.github.khopland.pgtasks.spi.StoreCapability}
     * on the class path that provides one of this type for this store. Made once per store and
     * type, the first time it is asked for: engines that share a store, as the nodes of a
     * {@code TaskTester} do, get the same one.
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
     * @return the job, to ask whether this node runs it now, or to have it run soon
     * @throws IllegalArgumentException if this engine has a job by that name already, its own
     *                                  included, such as {@code promotion}
     */
    Job job(HousekeepingJob job);

    /**
     * A job of this engine, as {@link #job} registered it. A job that needs it while it runs,
     * such as one that loops over batches, keeps it in a field of the extension.
     */
    interface Job {

        /** The job's name, as its {@link HousekeepingJob} gave it. */
        String name();

        /**
         * Whether this node runs the job now: while the engine runs, and for a
         * {@linkplain HousekeepingJob#leader leader job} only while this node holds its lease. A
         * job that loops over batches stops when it turns false.
         */
        boolean runsHere();

        /**
         * Runs the job once more, soon, besides its schedule, as if its interval had passed: for
         * work an operator asked for now, such as a recurring task to run now. It runs on the node
         * that runs the job: here, on the job's own thread once a run in progress ends, or, for a
         * leader job whose lease another node holds, on that node, told through the store's
         * notifications. While no node holds the lease, or if the notification is lost, it runs
         * at its next interval. Nothing runs while this engine isn't running.
         */
        void runSoon();
    }

    /**
     * Reloads something when it may have changed on another node: on a message on the channel
     * (sent with {@link Channel#send} in the transaction that changed it, so it arrives once
     * that commits), after the notification connection was lost, and on every heartbeat, since a
     * message may get lost. Runs on a thread of its own, never before the extensions' start
     * actions have run; requests that arrive while one waits are served by that one run. Channel
     * names are lowercase words ({@link io.github.khopland.pgtasks.spi.Notifier#CHANNEL});
     * {@code ready}, {@code topic} and {@code job} are the engine's.
     *
     * @return the channel, to send messages on
     */
    Channel channel(String name, Runnable reload);

    /**
     * Like {@link #channel(String, Runnable)}, told what the messages since the last run said:
     * their payloads, or an empty set when anything may have changed, after a lost connection,
     * on the heartbeat, or when more messages came in than it is worth listing.
     */
    Channel channel(String name, Reload reload);

    /** A reload told what changed; see {@link #channel(String, Reload)}. */
    @FunctionalInterface
    interface Reload {
        /** @param payloads the messages' payloads, or empty when anything may have changed */
        void reload(Set<String> payloads) throws Exception;
    }

    /** A notification channel, as {@link #channel} registered it. */
    interface Channel {

        /** The channel's name. */
        String name();

        /**
         * Sends a message on the channel in this transaction: once it commits, the reloads on the
         * channel get the payload, on every node of every application that shares the store, and
         * none do if it rolls back. Keep the payload short, such as a topic's name; see
         * {@link TaskStore#notify}.
         */
        void send(StoreTransaction transaction, String payload) throws SQLException;
    }

    /**
     * Shows operators what the extension knows and lets them act on it, through the starter's
     * endpoint and dashboard; see {@link OperatorView}.
     *
     * @throws IllegalArgumentException if this engine has a view by that name already
     */
    void operatorView(OperatorView view);

    /**
     * Runs each time the engine starts, before the channel reloads, jobs and topics do, so nothing
     * is claimed before the extension is ready; also after a restart. Extensions start in the
     * order they were installed, and each one's actions run in the order it registered them. One
     * that throws fails the start: the stop actions then run for the extensions that started
     * before it, not for this one.
     */
    void onStart(Callback action);

    /**
     * Runs each time the engine stops, once its topics, channel reloads and jobs have stopped, so
     * its gates, listeners, reloads and jobs are done with before it runs. Extensions stop in the
     * reverse order they were installed, and each one's actions run in the reverse order it
     * registered them, only if all its start actions ran. One that throws is logged, and the
     * others still run.
     */
    void onStop(Callback action);

    /** A start or stop action; see {@link #onStart} and {@link #onStop}. */
    @FunctionalInterface
    interface Callback {
        void run() throws Exception;
    }
}
