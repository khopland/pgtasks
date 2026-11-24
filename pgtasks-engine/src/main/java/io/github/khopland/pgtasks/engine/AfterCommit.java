package io.github.khopland.pgtasks.engine;

import java.util.List;

import org.jspecify.annotations.Nullable;

/** Runs the actions registered to run after a commit. */
final class AfterCommit {

    private AfterCommit() {
    }

    /**
     * Runs every action, even after one fails: with a store that doesn't join the application's
     * transactions, each one stores pushes made in the transaction, and one push that fails must
     * not drop the others. Throws the first failure, with the later ones suppressed.
     */
    static void runAll(List<Runnable> actions) {
        @Nullable RuntimeException failure = null;
        for (Runnable action : actions) {
            try {
                action.run();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
