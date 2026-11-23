package io.github.khopland.pgtasks;

import java.util.Objects;
import java.util.Set;

/**
 * Fan-in, staged by a handler with {@link Work#junction}. If the staging task continues, the
 * completion checks the group (these identifiers plus the staging task's own) in the target
 * topic, and once every identifier has a task there that counts, the group's suspended tasks in
 * that topic become ready.
 *
 * @param counts which tasks count as present
 */
public record Junction(TopicRef<?> topic, Counts counts, Set<String> identifiers) {

    /** Which tasks in the target topic count as present for an identifier. */
    public enum Counts {

        /** Only a suspended task: the classic "all parts have arrived and are waiting". (1.x: SINGULAR.) */
        SUSPENDED,

        /**
         * Any task, finished or not, except one that was made redundant. (1.x: ETERNAL.)
         *
         * <p>A finished task counts only while its row exists: once the topic's retention for its
         * outcome has passed (30 days for succeeded by default, see
         * {@code TopicSettings.DEFAULT_RETENTION}), housekeeping deletes it and the identifier is
         * missing again. A group whose parts finish further apart than that never completes. Keep
         * the target topic's retention longer than the time a group can take to arrive.
         */
        ANY
    }

    public Junction {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(counts, "counts");
        identifiers = Set.copyOf(identifiers);
    }
}
