package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Coalesced} under Fray's scheduler: runs never overlap, every request is served by a run
 * that starts after it, and nothing runs once stop has returned. Run with {@code -Pfray}.
 */
@ExtendWith(FrayTestExtension.class)
class CoalescedFrayTest {

    private static final int REQUESTERS = 3;

    @FrayTest(iterations = 1000)
    void every_request_is_served_by_a_later_run_and_runs_never_overlap() throws Exception {
        Progress progress = new Progress();
        AtomicInteger requested = new AtomicInteger(), servedUpTo = new AtomicInteger();
        AtomicInteger running = new AtomicInteger(), mostAtOnce = new AtomicInteger();
        Coalesced coalesced = new Coalesced("fray", "fray", () -> {
            mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
            // Everything requested before this run started is served by it.
            int seen = requested.get();
            running.decrementAndGet();
            progress.update(() -> servedUpTo.accumulateAndGet(seen, Math::max));
        }, Thread::ofPlatform);

        Thread[] requesters = new Thread[REQUESTERS];
        for (int i = 0; i < REQUESTERS; i++) {
            requesters[i] = Thread.ofPlatform().start(() -> {
                requested.incrementAndGet();
                coalesced.request();
            });
        }
        for (Thread requester : requesters) {
            requester.join();
        }

        progress.awaitUntil(() -> servedUpTo.get() == REQUESTERS);
        assertThat(mostAtOnce).as("runs at once").hasValue(1);
        coalesced.stop(Duration.ofSeconds(5));
    }

    @FrayTest(iterations = 1000)
    void nothing_runs_once_stop_returns_and_a_restart_serves_requests_again() throws Exception {
        Progress progress = new Progress();
        AtomicBoolean stopped = new AtomicBoolean();
        AtomicInteger runsAfterStop = new AtomicInteger(), runsAfterRestart = new AtomicInteger();
        AtomicBoolean restarted = new AtomicBoolean();
        Coalesced coalesced = new Coalesced("fray", "fray", () -> {
            if (restarted.get()) {
                progress.update(runsAfterRestart::incrementAndGet);
            } else if (stopped.get()) {
                runsAfterStop.incrementAndGet();
            }
        }, Thread::ofPlatform);

        Thread requester = Thread.ofPlatform().start(() -> {
            coalesced.request();
            coalesced.request();
        });
        coalesced.stop(Duration.ofSeconds(5));
        stopped.set(true);
        requester.join();
        assertThat(runsAfterStop).as("runs after stop returned").hasValue(0);

        coalesced.restart();
        restarted.set(true);
        coalesced.request();
        progress.awaitUntil(() -> runsAfterRestart.get() > 0);
        coalesced.stop(Duration.ofSeconds(5));
    }
}
