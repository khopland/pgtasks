package org.example.task2.spi;

/**
 * Delivers wake-ups when a topic gets ready work, and news of changed topics and rate limits.
 * Notifications are hints: if one is lost, polling still finds the work, and each heartbeat
 * reloads the topics and limits.
 */
public interface Notifier extends AutoCloseable {

    interface Listener {

        /** A topic got ready work. */
        void ready(int topicId);

        /** A topic was registered, paused or resumed. */
        default void topicChanged(String name) {
        }

        /** An operator changed the override of a rate limit. */
        default void limitChanged(String name) {
        }

        /** Notifications may have been missed, for example after a reconnect. */
        void missed();
    }

    void start(Listener listener);

    /**
     * Whether notifications are getting through now, for health checks. While they aren't, the
     * engine still finds work by polling, only later.
     */
    default boolean connected() {
        return true;
    }

    @Override
    void close();
}
