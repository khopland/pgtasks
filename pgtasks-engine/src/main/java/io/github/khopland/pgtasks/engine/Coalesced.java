package io.github.khopland.pgtasks.engine;

import java.time.Duration;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a job soon on a virtual thread of its own, for notifications, which mustn't wait on the
 * store. Requests that arrive before the job starts are served by that one run; one that arrives
 * while it runs gets one more run after it. Runs never overlap. After {@link #stop}, requests are
 * ignored.
 */
final class Coalesced {

    private static final Logger LOGGER = LoggerFactory.getLogger(Coalesced.class);

    private final String thread;
    private final String what;
    private final Housekeeper.Job job;
    /** Guarded by this: a run was asked for that hasn't started yet. */
    private boolean requested;
    /** Guarded by this: the thread that runs the job while there are requests, if any. */
    private @Nullable Thread running;
    private volatile boolean stopped;

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
        if (stopped) {
            return;
        }
        requested = true;
        if (running == null) {
            running = Thread.ofVirtual().name(thread).start(this::serve);
        }
    }

    /** Runs the job for as long as requests keep arriving, one run at a time. */
    private void serve() {
        while (true) {
            synchronized (this) {
                if (!requested || stopped) {
                    if (running == Thread.currentThread()) {
                        running = null;
                    }
                    return;
                }
                requested = false;
            }
            try {
                job.run();
            } catch (Exception | Error e) {
                if (!stopped) {
                    LOGGER.warn("Could not reload the {}; the next heartbeat tries again", what, e);
                }
            }
        }
    }

    /** Takes requests again after {@link #stop}, for an engine started again. */
    synchronized void restart() {
        stopped = false;
        requested = false;
        if (running != null && !running.isAlive()) {
            running = null;
        }
    }

    /** Ignores requests from now on, and interrupts and waits for a run in progress. */
    void stop(Duration patience) throws InterruptedException {
        Thread current;
        synchronized (this) {
            stopped = true;
            requested = false;
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
