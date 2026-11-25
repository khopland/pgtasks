package io.github.khopland.pgtasks.engine;

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

    /**
     * Whether a failure may pass if tried again, but may as well be the work's own fault: a
     * statement or lock timeout, the server out of a resource, a database that turned read-only
     * (a failover in progress). Worth a few more tries, not endless ones: a statement that always
     * takes longer than {@code statement_timeout} would otherwise be retried forever.
     */
    static boolean mayPass(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql && sql.getSQLState() != null) {
                String state = sql.getSQLState();
                // 57014 query canceled (statement_timeout), 55P03 lock not available (lock_timeout),
                // 53: insufficient resources, 25006 read-only transaction
                if (state.equals("57014") || state.equals("55P03") || state.startsWith("53") || state.equals("25006")) {
                    return true;
                }
            }
        }
        return false;
    }
}
