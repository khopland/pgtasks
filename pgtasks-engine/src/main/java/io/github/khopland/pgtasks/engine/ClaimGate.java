package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.Objects;

import io.github.khopland.pgtasks.TopicRef;

/**
 * Asked before every claim on this node: how many tasks a topic may claim now, or how long it
 * waits first. A local rate limit, or a circuit breaker, is a gate. With several gates the
 * strictest wins. Called on the topic's poll thread, often: keep it quick, and return
 * {@link Permit#ANY} rather than a new permit when nothing limits the claim.
 */
@FunctionalInterface
public interface ClaimGate {

    Permit permit(TopicRef<?> topic);

    /** After a claim it permitted: how many tasks it got, possibly none. */
    default void claimed(TopicRef<?> topic, int tasks) {
    }

    /**
     * A claim of at most {@code maxTasks}, or, with a delay, none now: the topic asks again after
     * the delay, or sooner when it is woken.
     */
    record Permit(int maxTasks, Duration delay) {

        /** Claim as the topic's batch size says. */
        public static final Permit ANY = new Permit(Integer.MAX_VALUE, Duration.ZERO);

        public Permit {
            Objects.requireNonNull(delay, "delay");
            if (maxTasks < 0 || delay.isNegative() || (maxTasks == 0) == delay.isZero()) {
                throw new IllegalArgumentException("A permit claims at least one task, or waits a positive time");
            }
        }

        public static Permit atMost(int tasks) {
            return new Permit(tasks, Duration.ZERO);
        }

        public static Permit waitFor(Duration delay) {
            return new Permit(0, delay);
        }

        public boolean waits() {
            return maxTasks == 0;
        }
    }
}
