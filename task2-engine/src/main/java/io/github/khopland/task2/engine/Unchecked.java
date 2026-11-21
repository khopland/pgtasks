package io.github.khopland.task2.engine;

import java.sql.SQLException;
import java.util.concurrent.Callable;

import io.github.khopland.task2.TaskStorageException;

/** Runs store work behind the public API, which throws only unchecked exceptions. */
final class Unchecked {

    @FunctionalInterface
    interface Action {
        void run() throws Exception;
    }

    private Unchecked() {
    }

    static <T> T call(String what, Callable<T> work) {
        try {
            return work.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TaskStorageException(what + " was interrupted", e);
        } catch (SQLException e) {
            throw new TaskStorageException(what + " failed in the database: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new TaskStorageException(what + " failed: " + e.getMessage(), e);
        }
    }

    static void run(String what, Action work) {
        call(what, () -> {
            work.run();
            return null;
        });
    }
}
