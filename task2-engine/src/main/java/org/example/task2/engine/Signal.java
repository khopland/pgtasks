package org.example.task2.engine;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** A wake-up that is remembered until someone waits for it, so none is lost between polls. */
final class Signal {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private boolean pending;

    void signal() {
        lock.lock();
        try {
            pending = true;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Waits until signalled or the timeout passes. Returns whether a signal arrived. */
    boolean await(Duration timeout) throws InterruptedException {
        long nanos = timeout.toNanos();
        lock.lock();
        try {
            while (!pending && nanos > 0) {
                nanos = condition.awaitNanos(nanos);
            }
            boolean signalled = pending;
            pending = false;
            return signalled;
        } finally {
            lock.unlock();
        }
    }
}
