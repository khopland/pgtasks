package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

/**
 * Runs a job soon on a virtual thread of its own, for notifications, which mustn't wait on the
 * store. Requests that arrive before the job starts are served by that one run; one that arrives
 * while it runs gets another run. After {@link #stop}, requests are ignored.
 */
final class Coalesced {

    private static final System.Logger LOGGER = System.getLogger(Coalesced.class.getName());

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
                        LOGGER.log(System.Logger.Level.WARNING, "Could not reload the " + what + "; the next heartbeat tries again", e);
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
                LOGGER.log(System.Logger.Level.WARNING, "Reloading the " + what + " ignored the stop and is still running");
            }
        }
    }
}
