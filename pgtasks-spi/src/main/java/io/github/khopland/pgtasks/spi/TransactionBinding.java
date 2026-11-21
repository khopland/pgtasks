package io.github.khopland.pgtasks.spi;

import java.util.Optional;

/**
 * Finds the application transaction the calling code runs in, so {@code Tasks.push} can join it
 * or hold its pushes back until it commits. A Spring integration resolves the current Spring
 * transaction.
 */
@FunctionalInterface
public interface TransactionBinding {

    Optional<AppTransaction> current();

    static TransactionBinding none() {
        return Optional::empty;
    }
}
