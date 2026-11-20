package org.example.task2.spi;

/**
 * Delivers wake-ups when a topic gets ready work, and news of changed rate limits. Notifications
 * are hints: polling still finds the work, and each heartbeat reloads the limits, if one is lost.
 */
public interface Notifier extends AutoCloseable {

    interface Listener {

        /** A topic got ready work. */
        void ready(int topicId);

        /** An operator changed the override of a rate limit. */
        default void limitChanged(String name) {
        }

        /** Notifications may have been missed, for example after a reconnect. */
        void missed();
    }

    void start(Listener listener);

    @Override
    void close();
}
