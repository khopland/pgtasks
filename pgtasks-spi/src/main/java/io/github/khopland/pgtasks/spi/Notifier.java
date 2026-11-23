package io.github.khopland.pgtasks.spi;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Delivers wake-ups when a topic gets ready work, and messages on named channels, such as
 * {@value #TOPIC} for topics registered or deleted. Notifications are hints: if one is lost,
 * polling still finds the work, and each heartbeat reloads what the channels report.
 */
public interface Notifier extends AutoCloseable {

    /** The channel the store sends a topic's name on when it is registered or deleted. */
    String TOPIC = "topic";

    /** What a channel name looks like: a lowercase word, so it can be part of a database identifier. */
    Pattern CHANNEL = Pattern.compile("[a-z][a-z0-9_]{0,30}");

    interface Listener {

        /** A topic got ready work. */
        void ready(int topicId);

        /** A message on one of the channels the notifier was started with. */
        default void notified(String channel, String payload) {
        }

        /** Notifications may have been missed, for example after a reconnect. */
        void missed();
    }

    /** Starts delivering wake-ups, and messages on these channels, to the listener. */
    void start(Listener listener, Set<String> channels);

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
