package io.github.khopland.task2.engine;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

/** Whether a failure is worth retrying: the connection or the server went away, or a conflict that resolves itself. */
final class Transient {

    private Transient() {
    }

    static boolean is(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLTransientException || cause instanceof SQLRecoverableException) {
                return true;
            }
            if (cause instanceof SQLException sql && sql.getSQLState() != null) {
                String state = sql.getSQLState();
                // 08: connection exception, 57P0x: shutdown or crash, 40001 serialization, 40P01 deadlock
                if (state.startsWith("08") || state.startsWith("57P0") || state.equals("40001") || state.equals("40P01")) {
                    return true;
                }
            }
        }
        return false;
    }
}
