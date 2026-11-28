package io.github.khopland.pgtasks.engine.extension;

import java.util.List;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.spi.StoreTransaction;

/**
 * Told of every batch completion on this node, inside the transaction that records it, after its
 * tasks' results and downstream pushes are written and before it commits: for what must commit
 * with the completion, such as an audit row in the extension's {@linkplain ExtensionContext#data
 * data}, or the next step of a workflow pushed with {@link ExtensionContext#tasks(StoreTransaction)}.
 * Where a {@link BatchListener} hears of a batch once it has committed, this is part of it.
 *
 * <p>One that throws fails the completion: it rolls back, and the batch's tasks fail with that
 * error and are retried, as when the database refuses a completion. A completion that fails
 * on a transient error is tried again, and this is called again in the new transaction. It isn't
 * called for the failure that is then recorded. Called on the batch's thread: keep it quick.
 */
@FunctionalInterface
public interface CompletionHook {

    void completing(Completing batch) throws Exception;

    /**
     * A batch's completion about to commit. The engine implements this; minor releases may add to it.
     */
    interface Completing {

        TopicRef<?> topic();

        /** What becomes of each task the batch ran, as a {@link BatchListener} is told after commit. */
        List<BatchListener.Completed> tasks();

        /** The transaction that records the completion. */
        StoreTransaction transaction();
    }
}
