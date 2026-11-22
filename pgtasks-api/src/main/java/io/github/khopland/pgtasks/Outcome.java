package io.github.khopland.pgtasks;

import java.util.Locale;

/**
 * How a finished task ended, as recorded in pgtasks.task. {@link #CANCELLED}: an operator took it
 * out of the queue before it ran (schema version 2).
 */
public enum Outcome {
    SUCCEEDED, FILTERED, FAILED, REDUNDANT, RECREATED, CANCELLED;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
