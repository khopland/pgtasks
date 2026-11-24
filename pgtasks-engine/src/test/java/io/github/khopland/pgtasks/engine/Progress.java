package io.github.khopland.pgtasks.engine;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Lets a Fray test wait for a condition other threads bring about, without a timeout: when no
 * thread can make progress any more, Fray reports a deadlock with the schedule that led to it, so a
 * lost wake-up fails the test instead of hanging it or passing after a sleep.
 */
final class Progress {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    /** Runs the update under the lock and wakes the waiters. */
    void update(Runnable update) {
        lock.lock();
        try {
            update.run();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void awaitUntil(BooleanSupplier done) throws InterruptedException {
        lock.lock();
        try {
            while (!done.getAsBoolean()) {
                changed.await();
            }
        } finally {
            lock.unlock();
        }
    }
}
