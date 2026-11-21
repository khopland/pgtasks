package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a job soon on a virtual thread of its own, for notifications, which mustn't wait on the
 * store. Requests that arrive before the job starts are served by that one run; one that arrives
 * while it runs gets another run. After {@link #stop}, requests are ignored.
 */
final class Coalesced {

    private static final Logger LOGGER = LoggerFactory.getLogger(Coalesced.class);

    private final String thread;
    private final String what;
    private final Housekeeper.Job job;
    private final AtomicBoolean pending = new AtomicBoolean();
    private volatile boolean stopped;
    private volatile @Nullable Thread running;

    /**
     * @param thread names the thread, after "pgtasks-"
     * @param what   what the job reloads, for the log
     */
    Coalesced(String thread, String what, Housekeeper.Job job) {
        this.thread = "pgtasks-" + thread;
        this.what = what;
        this.job = job;
    }

    synchronized void request() {
        if (!stopped && pending.compareAndSet(false, true)) {
            running = Thread.ofVirtual().name(thread).start(() -> {
                pending.set(false);
                try {
                    job.run();
                } catch (Exception e) {
                    if (!stopped) {
                        LOGGER.warn("Could not reload the {}; the next heartbeat tries again", what, e);
                    }
                }
            });
        }
    }

    /** Takes requests again after {@link #stop}, for an engine started again. */
    synchronized void restart() {
        stopped = false;
        pending.set(false);
    }

    /** Ignores requests from now on, and interrupts and waits for a run in progress. */
    void stop(Duration patience) throws InterruptedException {
        Thread current;
        synchronized (this) {
            stopped = true;
            current = running;
        }
        if (current != null && current.isAlive()) {
            current.interrupt();
            if (!current.join(patience)) {
                LOGGER.warn("Reloading the {} ignored the stop and is still running", what);
            }
        }
    }
}
