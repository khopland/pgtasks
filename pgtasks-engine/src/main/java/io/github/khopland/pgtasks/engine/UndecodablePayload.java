package io.github.khopland.pgtasks.engine;

import org.jspecify.annotations.Nullable;

/**
 * A payload the topic's codec can't read. Retrying won't change the stored text, so the task is
 * dead-lettered at once; once a codec that reads it is deployed, reviving it (or a topic that
 * revives on a new version) gives it another go.
 */
final class UndecodablePayload extends RuntimeException {

    UndecodablePayload(@Nullable String payloadType, Throwable cause) {
        super("Could not decode the payload" + (payloadType == null ? "" : " of type " + payloadType) + ": " + cause, cause);
    }
}
