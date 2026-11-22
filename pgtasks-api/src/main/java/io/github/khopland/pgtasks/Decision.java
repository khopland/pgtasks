package io.github.khopland.pgtasks;

import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * What happened to a task. Only {@link #success()} continues: its downstream work is enqueued.
 *
 * <p>When several sources decide on the same task (a handler and the batch resources that
 * review it), the most severe decision wins, in this order:
 * {@code failed > unavailable > suspended > throttled > later > filtered > success}. Between two delays of the
 * same kind, the longer one wins.
 */
public sealed interface Decision {

    /** Done. Downstream work is enqueued. */
    static Decision success() {
        return Success.INSTANCE;
    }

    /**
     * Done, with a result to keep on the task, such as a receipt or a reference in another
     * system: stored in {@code pgtasks.task.output}, shown by {@code admin().find(id)}.
     */
    static Decision success(String output) {
        return new Success(Objects.requireNonNull(output, "output"));
    }

    /** Irrelevant, so processing stops here. Not a failure, and nothing downstream runs. */
    static Decision filtered(@Nullable String reason) {
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

    /**
     * Not the task's fault: a downstream system is down, a connection failed. The task is retried
     * with the topic's backoff and its error is kept, but the attempt doesn't count, so an outage
     * can't use up a task's attempts.
     */
    static Decision unavailable(Throwable cause) {
        return new Unavailable(cause.getMessage(), cause);
    }

    static Decision unavailable(String message) {
        return new Unavailable(message, null);
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

    /** @param output what to keep on the task, or null */
    record Success(@Nullable String output) implements Decision {
        static final Success INSTANCE = new Success(null);

        @Override
        public int severity() {
            return 0;
        }
    }

    record Filtered(@Nullable String reason) implements Decision {
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

    record Unavailable(@Nullable String message, @Nullable Throwable cause) implements Decision {
        @Override
        public int severity() {
            return 5;
        }
    }

    record Failed(@Nullable String message, @Nullable Throwable cause) implements Decision {
        @Override
        public int severity() {
            return 6;
        }
    }
}
