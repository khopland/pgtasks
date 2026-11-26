package io.github.khopland.pgtasks;

import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * What happened to a task. Only {@link #success()} continues: its downstream work is enqueued.
 *
 * <p>When several sources decide on the same task (a handler and the batch resources that
 * review it), the most severe decision wins, in this order:
 * {@code failed > suspended > later > filtered > success}. Between two delays, the longer one wins.
 */
public sealed interface Decision {

    /** Done. Downstream work is enqueued. */
    static Decision success() {
        return Success.INSTANCE;
    }

    /**
     * Done, with a result to keep on the task, such as a receipt or a reference in another
     * system: stored in {@code pgtasks.task.output}, shown by {@code admin().find(id)}. The engine
     * keeps the first 65,536 characters of a longer one, with a note of how many more there were;
     * put a document elsewhere and keep a reference to it here.
     */
    static Decision success(String output) {
        return new Success(Objects.requireNonNull(output, "output"));
    }

    /** Irrelevant, so processing stops here. Not a failure, and nothing downstream runs. */
    static Decision filtered(@Nullable String reason) {
        return new Filtered(reason);
    }

    /**
     * A prerequisite is missing; the task waits until it is released. An operator releases it
     * (the engine's {@code admin().release(...)}, the Actuator endpoint or the dashboard), or code
     * pushes it again with {@link Insertion#REPLACE}. Nothing releases it on its own, so use
     * {@link #later(Duration)} to wait for a time. Never uses up an attempt.
     */
    static Decision suspended() {
        return Suspended.INSTANCE;
    }

    /**
     * Not yet: the task runs again after the delay, at most a thousand years; to wait for
     * something rather than a time, use {@link #suspended()}. Never uses up an attempt. Like a delayed
     * push, it doesn't hold back later tasks under {@link Ordering#SINGULAR_BY_IDENTIFIER}.
     */
    static Decision later(Duration delay) {
        return new Later(delay);
    }

    /** Uses up an attempt; the topic's retry policy chooses between backoff and dead letter. */
    static Decision failed(Throwable cause) {
        return new Failed(messageOf(cause), cause);
    }

    /**
     * The cause's message, or null if asking for it throws: the engine records failures with this
     * from its own catch blocks, where a second exception would lose the batch's completion.
     */
    private static @Nullable String messageOf(Throwable cause) {
        try {
            return cause.getMessage();
        } catch (RuntimeException | LinkageError | StackOverflowError e) {
            return null;
        }
    }

    static Decision failed(String message) {
        return new Failed(message, null);
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
            Waits.checkDelay("delay", delay);
        }

        @Override
        public int severity() {
            return 2;
        }
    }

    record Suspended() implements Decision {
        static final Suspended INSTANCE = new Suspended();

        @Override
        public int severity() {
            return 3;
        }
    }

    record Failed(@Nullable String message, @Nullable Throwable cause) implements Decision {
        @Override
        public int severity() {
            return 4;
        }
    }
}
