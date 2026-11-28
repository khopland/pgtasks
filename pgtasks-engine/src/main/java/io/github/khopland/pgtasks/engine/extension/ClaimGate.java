package io.github.khopland.pgtasks.engine.extension;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.TopicRef;

/**
 * Asked before every claim on this node: how many tasks a topic may claim now, or how long it
 * waits first. A local rate limit, a pause or a circuit breaker is a gate. With several gates the
 * strictest wins, and the claim is a {@linkplain Permit#probe probe} if any of them says so.
 * Called on the topic's poll thread, often: keep it quick, and return {@link Permit#ANY} rather
 * than a new permit when nothing limits the claim.
 *
 * <p>A gate that throws is logged and skipped, and the claim goes ahead without it, so a gate
 * that must hold claims back when its own storage fails catches the error and returns a wait.
 * While a gate holds a topic back, the engine's health, the starter's endpoint and the metrics
 * show it, under the name of the extension that registered it.
 */
@FunctionalInterface
public interface ClaimGate {

    /**
     * How many of the claim's tasks it allows, knowing the claim takes at most {@code wanted}: the
     * topic's batch size, or less when an earlier gate allowed less. A gate that reserves a shared
     * allowance reserves no more than this.
     */
    Permit permit(TopicRef<?> topic, int wanted);

    /**
     * Once after every permit that didn't wait: the ids of the tasks the claim got, which a
     * {@link BatchListener} later reports on. Empty when it got none, and also when no claim was
     * made because another gate said wait or the claim failed, so a gate can hand back what it
     * reserved.
     */
    default void claimed(TopicRef<?> topic, List<Long> taskIds) {
    }

    /**
     * A claim of at most {@code maxTasks}, or, with a delay, none now: the topic asks again after
     * the delay, or sooner when it is {@linkplain ExtensionContext#wake woken}.
     *
     * @param probing whether the claim is a probe: see {@link #probe(int)}
     */
    record Permit(int maxTasks, Duration delay, boolean probing) {

        /** Claim as the topic's batch size says. */
        public static final Permit ANY = new Permit(Integer.MAX_VALUE, Duration.ZERO, false);

        public Permit {
            Objects.requireNonNull(delay, "delay");
            if (maxTasks < 0 || delay.isNegative() || (maxTasks == 0) == delay.isZero()) {
                throw new IllegalArgumentException("A permit claims at least one task, or waits a positive time");
            }
            if (probing && maxTasks == 0) {
                throw new IllegalArgumentException("A probe claims at least one task");
            }
        }

        /** A permit that isn't a probe. */
        public Permit(int maxTasks, Duration delay) {
            this(maxTasks, delay, false);
        }

        public static Permit atMost(int tasks) {
            return new Permit(tasks, Duration.ZERO);
        }

        public static Permit waitFor(Duration delay) {
            return new Permit(0, delay);
        }

        /**
         * A claim of at most this many tasks to find out whether the topic works again, as a
         * circuit breaker sends while it holds the topic back. A failure of a probed task doesn't
         * use up an attempt, even one the retry policy counts: the task retries with the backoff of
         * a failure that isn't counted. A failure the policy dead-letters at once still does, and a
         * task with no attempts left is still dead-lettered.
         */
        public static Permit probe(int tasks) {
            return new Permit(tasks, Duration.ZERO, true);
        }

        public boolean waits() {
            return maxTasks == 0;
        }
    }
}
