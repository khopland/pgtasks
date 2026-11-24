package io.github.khopland.pgtasks.engine;

import java.time.Duration;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Signal} under Fray's scheduler: a signal is remembered until the waiter takes it, and a
 * signal after a {@link Signal#count()} reading changes the count, which
 * {@link TopicWorker#quietSince} relies on. Run with {@code -Pfray}.
 */
@ExtendWith(FrayTestExtension.class)
class SignalFrayTest {

    @FrayTest(iterations = 1000)
    void a_signal_sent_before_or_during_the_wait_wakes_the_waiter() throws Exception {
        Signal signal = new Signal();
        long before = signal.count();
        Thread signaller = Thread.ofPlatform().start(signal::signal);

        // Long enough that only a lost signal ends the wait by timing out.
        assertThat(signal.await(Duration.ofHours(1))).as("woken by the signal").isTrue();
        assertThat(signal.count()).as("count after a signal").isGreaterThan(before);
        signaller.join();
    }

    @FrayTest(iterations = 1000)
    void a_wake_up_taken_by_one_wait_isnt_seen_by_the_next() throws Exception {
        Signal signal = new Signal();
        Thread signaller = Thread.ofPlatform().start(signal::signal);
        signaller.join();

        assertThat(signal.await(Duration.ofHours(1))).isTrue();
        assertThat(signal.await(Duration.ZERO)).as("a second wait right after").isFalse();
    }
}
