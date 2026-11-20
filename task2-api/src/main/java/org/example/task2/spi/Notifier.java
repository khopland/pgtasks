package org.example.task2.spi;

/**
 * Delivers wake-ups when a topic gets ready work. Notifications are hints: polling still finds
 * the work if one is lost.
 */
public interface Notifier extends AutoCloseable {

    interface Listener {

        /** A topic got ready work. */
        void ready(int topicId);

        /** Notifications may have been missed, for example after a reconnect. */
        void missed();
    }

    void start(Listener listener);

    @Override
    void close();
}
