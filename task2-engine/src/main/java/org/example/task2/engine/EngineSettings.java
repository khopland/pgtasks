package org.example.task2.engine;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.example.task2.PayloadCodec;
import org.example.task2.spi.Notifier;
import org.example.task2.spi.TransactionBinding;

/**
 * Node-wide settings and collaborators. A placeholder for the configuration model of a later
 * phase; {@link #defaults()} matches the plan's defaults.
 *
 * @param nodeName           shown in tasks.node and tasks.task_view
 * @param heartbeat          how often this node reports that it is alive
 * @param expiry             after how long without a heartbeat another node's tasks are requeued
 * @param promoteInterval    how often due delayed tasks and retries become ready
 * @param maxInFlightBatches node-wide cap on batches in flight; keep it below the pool size
 * @param notifier           wake-ups; {@code null} relies on polling alone
 * @param transactions       finds the current transaction for {@code Tasks.push}
 * @param codecs             codecs for payload types of topics this node only pushes to
 * @param snapshotInterval   how often the housekeeping leader snapshots queue counts
 */
public record EngineSettings(
    String nodeName,
    Duration heartbeat,
    Duration expiry,
    Duration promoteInterval,
    int maxInFlightBatches,
    Notifier notifier,
    TransactionBinding transactions,
    Map<Class<?>, PayloadCodec<?>> codecs,
    Duration snapshotInterval
) {

    public EngineSettings {
        Objects.requireNonNull(nodeName, "nodeName");
        Objects.requireNonNull(heartbeat, "heartbeat");
        Objects.requireNonNull(expiry, "expiry");
        Objects.requireNonNull(promoteInterval, "promoteInterval");
        Objects.requireNonNull(transactions, "transactions");
        Objects.requireNonNull(snapshotInterval, "snapshotInterval");
        codecs = Map.copyOf(codecs);
        if (maxInFlightBatches < 1) {
            throw new IllegalArgumentException("maxInFlightBatches must be at least 1");
        }
        if (expiry.compareTo(heartbeat) <= 0) {
            throw new IllegalArgumentException("expiry must be longer than the heartbeat interval");
        }
    }

    /** Heartbeat every 15 s, requeue after 10 min, promote every second, 8 batches in flight. */
    public static EngineSettings defaults(String nodeName) {
        return new EngineSettings(nodeName, Duration.ofSeconds(15), Duration.ofMinutes(10), Duration.ofSeconds(1), 8, null, TransactionBinding.none(), Map.of(),
            Duration.ofMinutes(1));
    }

    public EngineSettings withHeartbeat(Duration heartbeat, Duration expiry) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval);
    }

    public EngineSettings withPromoteInterval(Duration promoteInterval) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval);
    }

    public EngineSettings withMaxInFlightBatches(int maxInFlightBatches) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval);
    }

    public EngineSettings withNotifier(Notifier notifier) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval);
    }

    public EngineSettings withTransactions(TransactionBinding transactions) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval);
    }

    public EngineSettings withSnapshotInterval(Duration snapshotInterval) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval);
    }

    public EngineSettings withCodecs(Map<Class<?>, PayloadCodec<?>> codecs) {
        return new EngineSettings(nodeName, heartbeat, expiry, promoteInterval, maxInFlightBatches, notifier, transactions, codecs, snapshotInterval);
    }
}
