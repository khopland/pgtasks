package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import io.github.khopland.pgtasks.spi.TaskStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tells every node about pushes this node saw commit, debounced. A push made with
 * {@link TaskStore.WakeUp#BY_CALLER} doesn't notify inside its own transaction, because Postgres
 * serializes the commits of all transactions that notified; this sends the notification after the
 * commit instead, in a short transaction of its own ({@link TaskStore#notifyReady}).
 *
 * <p>The first push after a quiet spell is sent at once; pushes within {@link #WINDOW} of a send
 * are gathered into the next one, so a node sends at most one notification transaction per window,
 * whatever its push rate. Only those transactions queue for the notify lock. A notification is a
 * hint: one that fails, or never goes out because the node stops, leaves the task to the next poll.
 *
 * <p>Works whether or not the engine was started: a node that only pushes still notifies. The
 * sender is a virtual thread that exists only while there is something to send.
 */
final class ReadyNotices {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadyNotices.class);

    static final Duration WINDOW = Duration.ofMillis(10);

    private final TaskStore store;
    private final long window;
    private final Supplier<Thread.Builder> threads;
    private final Set<Integer> pending = new LinkedHashSet<>();
    private boolean sending;
    private long lastSent = System.nanoTime() - Duration.ofDays(1).toNanos();
    /** Whether the last send failed, so a database that is down logs once, not once per window. */
    private volatile boolean failing;

    ReadyNotices(TaskStore store) {
        this(store, WINDOW);
    }

    ReadyNotices(TaskStore store, Duration window) {
        this(store, window, Thread::ofVirtual);
    }

    /** With threads of the given kind: the Fray tests need platform threads. */
    ReadyNotices(TaskStore store, Duration window, Supplier<Thread.Builder> threads) {
        this.store = store;
        this.window = window.toNanos();
        this.threads = threads;
    }

    /** A push to the topic committed with ready tasks. */
    void ready(int topicId) {
        synchronized (this) {
            pending.add(topicId);
            if (sending) {
                return;
            }
            sending = true;
        }
        threads.get().name("pgtasks-ready-notices").start(this::send);
    }

    private void send() {
        while (true) {
            long wait;
            synchronized (this) {
                if (pending.isEmpty()) {
                    sending = false;
                    return;
                }
                wait = lastSent + window - System.nanoTime();
            }
            if (wait > 0) {
                try {
                    Thread.sleep(Duration.ofNanos(wait));
                } catch (InterruptedException e) {
                    synchronized (this) {
                        sending = false;
                    }
                    return;
                }
            }
            List<Integer> topics;
            synchronized (this) {
                topics = List.copyOf(pending);
                pending.clear();
                lastSent = System.nanoTime();
            }
            sendNow(topics);
        }
    }

    private void sendNow(List<Integer> topics) {
        try {
            EngineCore.transact(store, transaction -> {
                store.notifyReady(transaction, topics);
                return null;
            });
            if (failing) {
                failing = false;
                LOGGER.info("Wake-up notifications are sent again");
            }
        } catch (Exception | Error e) {
            // An Error too: thrown out of send(), it would leave sending set, and no wake-up would go out again.
            if (!failing) {
                failing = true;
                LOGGER.warn("Could not send a wake-up notification; other nodes find the tasks at their next poll. Logged once until "
                    + "one is sent again.", e);
            }
        }
    }
}
