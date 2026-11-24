package io.github.khopland.pgtasks.engine;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.spi.StoreTransaction;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ReadyNotices} under Fray's scheduler: a topic marked ready is always notified by a send
 * that starts after it, whatever the sender is doing at the time, and sends never overlap. Run
 * with {@code -Pfray}.
 */
@ExtendWith(FrayTestExtension.class)
class ReadyNoticesFrayTest {

    @FrayTest(iterations = 1000)
    void no_ready_topic_is_lost_while_the_sender_winds_down() throws Exception {
        assertEveryTopicIsNotified(Duration.ZERO, List.of(1, 2, 3));
    }

    @FrayTest(iterations = 1000)
    void no_ready_topic_is_lost_while_the_sender_waits_out_the_window() throws Exception {
        assertEveryTopicIsNotified(Duration.ofMillis(1), List.of(1, 2, 1));
    }

    private static void assertEveryTopicIsNotified(Duration window, List<Integer> topics) throws Exception {
        RecordingStore recording = new RecordingStore();
        ReadyNotices notices = new ReadyNotices(recording.store(), window, Thread::ofPlatform);

        List<Thread> pushers = new ArrayList<>();
        for (int topic : topics) {
            pushers.add(Thread.ofPlatform().start(() -> notices.ready(topic)));
        }
        for (Thread pusher : pushers) {
            pusher.join();
        }

        recording.progress.awaitUntil(() -> recording.notified.containsAll(topics));
        assertThat(recording.mostAtOnce).as("sends at once").hasValue(1);
    }

    /** A store that only records the topics it is asked to notify. */
    private static final class RecordingStore {

        final Progress progress = new Progress();
        final Set<Integer> notified = new HashSet<>();
        final AtomicInteger sending = new AtomicInteger(), mostAtOnce = new AtomicInteger();

        TaskStore store() {
            StoreTransaction transaction = new StoreTransaction() {
                @Override
                public void commit() {
                }

                @Override
                public void rollback() {
                }

                @Override
                public Connection connection() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void close() {
                }
            };
            return (TaskStore) Proxy.newProxyInstance(TaskStore.class.getClassLoader(), new Class<?>[] {TaskStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "begin" -> transaction;
                    case "notifyReady" -> {
                        mostAtOnce.accumulateAndGet(sending.incrementAndGet(), Math::max);
                        @SuppressWarnings("unchecked")
                        Collection<Integer> ids = (Collection<Integer>) arguments[1];
                        sending.decrementAndGet();
                        progress.update(() -> notified.addAll(ids));
                        yield null;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        }
    }
}
