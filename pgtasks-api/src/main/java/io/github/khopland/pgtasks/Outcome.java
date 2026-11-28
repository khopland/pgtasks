package io.github.khopland.pgtasks;

import java.util.Locale;

/** How a finished task ended, as recorded in pgtasks.task. */
public enum Outcome {
    SUCCEEDED, FILTERED, FAILED, REDUNDANT, RECREATED,

    /** Taken out of the queue by an operator before it ran to an end; see {@code TaskAdmin.cancel}. */
    CANCELLED;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
