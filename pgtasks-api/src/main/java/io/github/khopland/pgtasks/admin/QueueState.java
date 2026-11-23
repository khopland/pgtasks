package io.github.khopland.pgtasks.admin;

import java.util.Locale;

/** Where an unfinished task is, as {@link TaskView#state()} names it while the task is unfinished. */
public enum QueueState {
    /** Delayed: runs at a time. */
    SCHEDULED,
    /** Backing off after a failure or throttling. */
    RETRYING,
    /** Waits for a worker. */
    READY,
    /** Claimed: a node runs it now. */
    ACTIVE,
    /** Waits until something releases it. */
    SUSPENDED,
    /** A dead letter: out of attempts, waits for an operator. */
    FAILED;

    /** The name as stored and shown: {@code scheduled}, {@code ready}, ... */
    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** The state with this label, whatever its case. */
    public static QueueState of(String label) {
        try {
            return valueOf(label.strip().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown queue state '" + label + "': scheduled, retrying, ready, active, suspended or failed", e);
        }
    }
}
