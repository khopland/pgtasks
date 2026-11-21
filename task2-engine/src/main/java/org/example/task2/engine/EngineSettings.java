package org.example.task2.engine;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.example.task2.PayloadCodec;
import org.example.task2.spi.Notifier;
import org.example.task2.spi.TransactionBinding;

/**
 * Node-wide settings and collaborators, as the builder resolves them from {@link TaskGlobals};
 * {@link #defaults(String)} holds the built-in defaults.
 *
 * @param nodeName           shown in tasks.node and tasks.task_view
 * @param heartbeat          how often this node reports that it is alive
 * @param expiry             after how long without a heartbeat another node's tasks are requeued
 * @param promoteInterval    how often due delayed tasks and retries become ready
 * @param maxInFlightBatches node-wide cap on claims and completions talking to the store at once;
 *                           keep it below the pool size. Handlers run outside it, so a topic
 *                           whose handlers hang can't hold up the node's other topics
 * @param notifier           wake-ups; {@code null} relies on polling alone
 * @param transactions       finds the current transaction for {@code Tasks.push}
 * @param codecs             codecs for payload types of topics this node only pushes to
 * @param snapshotInterval   how often the housekeeping leader snapshots queue counts
 * @param purgeInterval      how often the housekeeping leader deletes finished tasks past their retention
 * @param observer           sees pushes and batches, for metrics and tracing
 * @param expiries           what happens to a task whose node dies under it: how long it waits,
 *                           and at which expiry it is dead-lettered
 */
record EngineSettings(
    String nodeName,
    Duration heartbeat,
    Duration expiry,
    Duration promoteInterval,
    int maxInFlightBatches,
    Notifier notifier,
    TransactionBinding transactions,
    Map<Class<?>, PayloadCodec<?>> codecs,
    Duration snapshotInterval,
    Duration purgeInterval,
    TaskObserver observer,
    org.example.task2.spi.TaskStore.Expiries expiries
) {

    public EngineSettings {
        Objects.requireNonNull(nodeName, "nodeName");
        Objects.requireNonNull(heartbeat, "heartbeat");
        Objects.requireNonNull(expiry, "expiry");
        Objects.requireNonNull(promoteInterval, "promoteInterval");
        Objects.requireNonNull(transactions, "transactions");
        Objects.requireNonNull(snapshotInterval, "snapshotInterval");
        Objects.requireNonNull(purgeInterval, "purgeInterval");
        observer = SafeObserver.of(Objects.requireNonNull(observer, "observer"));
        Objects.requireNonNull(expiries, "expiries");
        codecs = Map.copyOf(codecs);
        if (maxInFlightBatches < 1) {
            throw new IllegalArgumentException("maxInFlightBatches must be at least 1");
        }
        if (expiry.compareTo(heartbeat) <= 0) {
            throw new IllegalArgumentException("expiry must be longer than the heartbeat interval");
        }
    }

    /**
     * Heartbeat every 15 s, requeue after 10 min, promote every second, 8 batches in flight,
     * snapshot counts and apply retention every minute.
     */
    public static EngineSettings defaults(String nodeName) {
        return new EngineSettings(nodeName, Duration.ofSeconds(15), Duration.ofMinutes(10), Duration.ofSeconds(1), 8, null, TransactionBinding.none(), Map.of(),
            Duration.ofMinutes(1), Duration.ofMinutes(1), TaskObserver.NONE, Housekeeper.EXPIRIES);
    }

    /** How long the housekeeping lease lasts unless renewed: four heartbeats. */
    public Duration leaseTerm() {
        return heartbeat.multipliedBy(4);
    }

    public EngineSettings withHeartbeat(Duration heartbeat, Duration expiry) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withPromoteInterval(Duration promoteInterval) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withMaxInFlightBatches(int maxInFlightBatches) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withNotifier(Notifier notifier) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withTransactions(TransactionBinding transactions) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withSnapshotInterval(Duration snapshotInterval) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withPurgeInterval(Duration purgeInterval) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withCodecs(Map<Class<?>, PayloadCodec<?>> codecs) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withObserver(TaskObserver observer) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }

    public EngineSettings withExpiries(org.example.task2.spi.TaskStore.Expiries expiries) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval, purgeInterval, observer, expiries);
    }
}
