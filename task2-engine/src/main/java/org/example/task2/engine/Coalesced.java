package org.example.task2.engine;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs a job soon on a virtual thread of its own, for notifications, which mustn't wait on the
 * store. Requests that arrive before the job starts are served by that one run; one that arrives
 * while it runs gets another run.
 */
final class Coalesced {

    private static final System.Logger LOGGER = System.getLogger(Coalesced.class.getName());

    private final String name;
    private final Housekeeper.Job job;
    private final AtomicBoolean pending = new AtomicBoolean();

    /** @param name for the thread and the log, such as "limits" */
    Coalesced(String name, Housekeeper.Job job) {
        this.name = name;
        this.job = job;
    }

    void request() {
        if (pending.compareAndSet(false, true)) {
            Thread.ofVirtual().name("task2-" + name).start(() -> {
                pending.set(false);
                try {
                    job.run();
                } catch (Exception e) {
                    LOGGER.log(System.Logger.Level.WARNING, "Could not reload the " + name + "; the next heartbeat tries again", e);
                }
            });
        }
    }
}
