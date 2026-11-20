package org.example.task2;

import java.time.Duration;
import java.util.Objects;

/**
 * What happened to a task. Only {@link #success()} continues: its downstream work is enqueued.
 *
 * <p>When several sources decide on the same task (a handler and the batch resources that
 * review it), the most severe decision wins, in this order:
 * {@code failed > suspended > throttled > later > filtered > success}. Between two delays of the
 * same kind, the longer one wins.
 */
public sealed interface Decision {

    /** Done. Downstream work is enqueued. */
    static Decision success() {
        return Success.INSTANCE;
    }

    /** Irrelevant, so processing stops here. Not a failure, and nothing downstream runs. */
    static Decision filtered(String reason) {
        return new Filtered(reason);
    }

    /** A prerequisite is missing; the task waits until it is released. Never uses up an attempt. */
    static Decision suspended() {
        return Suspended.INSTANCE;
    }

    /**
     * Not yet: the task runs again after the delay. Never uses up an attempt. Like a delayed
     * push, it doesn't hold back later tasks under {@link Ordering#SINGULAR_BY_IDENTIFIER}.
     */
    static Decision later(Duration delay) {
        return new Later(delay);
    }

    /** Uses up an attempt; the topic's retry policy chooses between backoff and dead letter. */
    static Decision failed(Throwable cause) {
        return new Failed(cause.getMessage(), cause);
    }

    static Decision failed(String message) {
        return new Failed(message, null);
    }

    /** Downstream pushback: the task is retried after the delay and the attempt doesn't count. */
    static Decision throttled(Duration retryAfter) {
        return new Throttled(retryAfter);
    }

    /** Whether downstream work is enqueued. */
    default boolean continues() {
        return this instanceof Success;
    }

    /** Higher is more severe. */
    int severity();

    /** The more severe of the two decisions. */
    static Decision merge(Decision left, Decision right) {
        if (left.severity() != right.severity()) {
            return left.severity() > right.severity() ? left : right;
        }
        return switch (left) {
            case Later(Duration delay) when right instanceof Later(Duration other) && other.compareTo(delay) > 0 -> right;
            case Throttled(Duration delay) when right instanceof Throttled(Duration other) && other.compareTo(delay) > 0 -> right;
            default -> left;
        };
    }

    record Success() implements Decision {
        static final Success INSTANCE = new Success();

        @Override
        public int severity() {
            return 0;
        }
    }

    record Filtered(String reason) implements Decision {
        @Override
        public int severity() {
            return 1;
        }
    }

    record Later(Duration delay) implements Decision {
        public Later {
            Objects.requireNonNull(delay, "delay");
            if (delay.isNegative()) {
                throw new IllegalArgumentException("delay must not be negative");
            }
        }

        @Override
        public int severity() {
            return 2;
        }
    }

    record Throttled(Duration retryAfter) implements Decision {
        public Throttled {
            Objects.requireNonNull(retryAfter, "retryAfter");
            if (retryAfter.isNegative()) {
                throw new IllegalArgumentException("retryAfter must not be negative");
            }
        }

        @Override
        public int severity() {
            return 3;
        }
    }

    record Suspended() implements Decision {
        static final Suspended INSTANCE = new Suspended();

        @Override
        public int severity() {
            return 4;
        }
    }

    record Failed(String message, Throwable cause) implements Decision {
        @Override
        public int severity() {
            return 5;
        }
    }
}
