package io.github.khopland.pgtasks;

import java.util.Locale;

/** How a finished task ended, as recorded in tasks.task. */
public enum Outcome {
    SUCCEEDED, FILTERED, FAILED, REDUNDANT, RECREATED;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
