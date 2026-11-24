package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review 2026-11-24: a request that arrived while the job ran started a second run next to it, so
 * an extension's channel reload could run twice at once, and stop waited only for the newest.
 */
class CoalescedTest {

    @Test
    void a_request_while_the_job_runs_gets_one_more_run_after_it_never_beside_it() throws Exception {
        AtomicInteger running = new AtomicInteger(), mostAtOnce = new AtomicInteger(), runs = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1), release = new CountDownLatch(1);
        Coalesced coalesced = new Coalesced("test", "test", () -> {
            mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
            try {
                runs.incrementAndGet();
                firstStarted.countDown();
                release.await(5, TimeUnit.SECONDS);
            } finally {
                running.decrementAndGet();
            }
        });

        coalesced.request();
        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
        for (int request = 0; request < 10; request++) {
            coalesced.request();
        }
        Thread.sleep(100);
        assertThat(mostAtOnce).as("requests during a run wait for it").hasValue(1);
        release.countDown();

        EngineFixture.await("the second run", Duration.ofSeconds(5), () -> runs.get() == 2 && running.get() == 0);
        Thread.sleep(100);
        assertThat(runs).as("ten requests during the run are served by one more run").hasValue(2);
        assertThat(mostAtOnce).hasValue(1);
        coalesced.stop(Duration.ofSeconds(1));
    }

    @Test
    void stop_waits_for_the_run_in_progress_and_ignores_later_requests() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        Coalesced coalesced = new Coalesced("test", "test", () -> {
            runs.incrementAndGet();
            started.countDown();
            Thread.sleep(Duration.ofSeconds(10));
        });
        coalesced.request();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        coalesced.request();

        coalesced.stop(Duration.ofSeconds(5));
        coalesced.request();
        Thread.sleep(100);

        assertThat(runs).as("the pending request isn't served once stopped").hasValue(1);
        coalesced.restart();
        coalesced.request();
        EngineFixture.await("a run after the restart", Duration.ofSeconds(5), () -> runs.get() == 2);
        coalesced.stop(Duration.ofSeconds(5));
    }
}
