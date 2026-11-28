package io.github.khopland.pgtasks.engine.extension;

import java.util.List;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.spi.StoreTransaction;

/**
 * Told of every push on this node, inside the push's transaction, before its tasks are written:
 * a producer's, a handler's downstream work, an extension's own. It can check the tasks and
 * refuse the push by throwing, and write what must commit with the push in the same
 * transaction, such as a tenant's count in its {@linkplain ExtensionContext#data data}.
 *
 * <p>One that throws stops the push: nothing is written, and the exception reaches whoever
 * pushed, an unchecked one as it is and a checked one as a
 * {@link io.github.khopland.pgtasks.TaskStorageException}. A push in the application's own
 * transaction leaves that transaction for its owner to roll back. A refused downstream push fails
 * its batch's completion, as any failed push there does. Called on the pushing thread: keep it
 * quick, and keep what it writes small, since it runs in every push's transaction.
 */
@FunctionalInterface
public interface PushHook {

    void pushing(Pushing push) throws Exception;

    /**
     * A push about to be written. The engine implements this; minor releases may add to it.
     */
    interface Pushing {

        TopicRef<?> topic();

        Insertion insertion();

        /** The tasks as they were given, payloads not yet encoded. */
        List<Creation<?>> tasks();

        /**
         * The transaction the push writes in: the store's own, or one the store joined, such as
         * the application's; write in it with {@link ExtensionContext#data()} or
         * {@link ExtensionContext#tasks(StoreTransaction)}.
         */
        StoreTransaction transaction();
    }
}
