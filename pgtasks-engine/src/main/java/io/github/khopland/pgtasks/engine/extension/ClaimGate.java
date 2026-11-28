package io.github.khopland.pgtasks.engine.extension;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.TopicRef;

import org.jspecify.annotations.Nullable;

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
 * show it under the name of the extension that registered it: at once for a wait that gives a
 * {@linkplain Permit#waitFor(Duration, String) reason}, such as a pause, and otherwise once the
 * topic has been held for its poll interval without a claim, so a rate limit spacing claims out
 * doesn't show.
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
     * A claim of at most some number of tasks, or none now and a wait: the topic asks again after
     * the delay, or sooner when it is {@linkplain ExtensionContext#wake woken}. Made with
     * {@link #ANY}, {@link #atMost}, {@link #waitFor} and {@link #probe}.
     */
    final class Permit {

        /** Claim as the topic's batch size says. */
        public static final Permit ANY = new Permit(Integer.MAX_VALUE, Duration.ZERO, false, null);

        private final int maxTasks;
        private final Duration delay;
        private final boolean probing;
        private final @Nullable String reason;

        /** A claim of at most {@code maxTasks}, or, with zero tasks and a delay, a wait; see {@link #atMost} and {@link #waitFor}. */
        public Permit(int maxTasks, Duration delay) {
            this(maxTasks, delay, false, null);
        }

        private Permit(int maxTasks, Duration delay, boolean probing, @Nullable String reason) {
            Objects.requireNonNull(delay, "delay");
            if (probing && maxTasks < 1) {
                throw new IllegalArgumentException("A probe claims at least one task, not " + maxTasks);
            }
            if (maxTasks < 0 || delay.isNegative() || (maxTasks == 0) == delay.isZero()) {
                throw new IllegalArgumentException("A permit claims at least one task, or waits a positive time");
            }
            if (reason != null && reason.isBlank()) {
                throw new IllegalArgumentException("A reason to wait says something, such as \"paused\"");
            }
            this.maxTasks = maxTasks;
            this.delay = delay;
            this.probing = probing;
            this.reason = reason == null ? null : reason.strip();
        }

        public static Permit atMost(int tasks) {
            return new Permit(tasks, Duration.ZERO);
        }

        /** No claim now: the topic asks again after the delay, or when it is woken. */
        public static Permit waitFor(Duration delay) {
            return new Permit(0, delay);
        }

        /**
         * Like {@link #waitFor(Duration)}, saying why in a few words, such as {@code "paused"} or
         * {@code "breaker open"}: a hold rather than a throttle. The health, the endpoint and the
         * dashboard show the topic held back at once, with the reason, where a wait without one
         * shows only once the topic has been held for its poll interval.
         */
        public static Permit waitFor(Duration delay, String reason) {
            return new Permit(0, delay, false, Objects.requireNonNull(reason, "reason"));
        }

        /**
         * A claim of at most this many tasks to find out whether the topic works again, as a
         * circuit breaker sends while it holds the topic back. A failure of a probed task doesn't
         * use up an attempt, even one the retry policy counts: the task retries with the backoff of
         * a failure that isn't counted. A failure the policy dead-letters at once still does, and a
         * task with no attempts left is still dead-lettered.
         */
        public static Permit probe(int tasks) {
            return new Permit(tasks, Duration.ZERO, true, null);
        }

        /** At most how many tasks the claim takes; zero when it waits. */
        public int maxTasks() {
            return maxTasks;
        }

        /** How long the topic waits before it asks again; zero when it claims. */
        public Duration delay() {
            return delay;
        }

        /** Whether the claim is a {@linkplain #probe probe}. */
        public boolean probing() {
            return probing;
        }

        /** Why it waits, if the gate said; see {@link #waitFor(Duration, String)}. */
        public @Nullable String reason() {
            return reason;
        }

        public boolean waits() {
            return maxTasks == 0;
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return other instanceof Permit permit && permit.maxTasks == maxTasks && permit.delay.equals(delay) && permit.probing == probing
                && Objects.equals(permit.reason, reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxTasks, delay, probing, reason);
        }

        @Override
        public String toString() {
            if (waits()) {
                return "Permit[wait " + delay + (reason == null ? "" : ", " + reason) + "]";
            }
            return "Permit[" + (maxTasks == Integer.MAX_VALUE ? "any" : "at most " + maxTasks) + (probing ? ", probe" : "") + "]";
        }
    }
}
