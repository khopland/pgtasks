package io.github.khopland.task2.engine;

import java.time.Duration;
import java.util.Objects;

/**
 * When a topic stops claiming because most of its recent tasks fail: a downstream system is
 * probably down, and claiming more only fails more. While open, the node sends one probe at a
 * time, a single task every {@code probeEvery}, and a probe's failure doesn't use up an attempt;
 * the first probe that succeeds closes it. Per node: each node opens and closes its own.
 *
 * <p>A single task that keeps failing doesn't open it, however often it retries: it takes
 * failures from at least {@code minTasks} different tasks, so a poison task still reaches the
 * dead letters while the rest of the topic runs. Failures the retry policy dead-letters at once
 * ({@link Retry#deadLetterAt}), such as bad input, don't count at all: they are the task's fault,
 * not an outage.
 *
 * @param opensAt    the share of the last {@code window} results that must be failures
 * @param window     how many recent results count; 0 turns the breaker off
 * @param minTasks   how many different tasks must have failed among them
 * @param probeEvery how long to wait between probes while open
 */
public record Breaker(double opensAt, int window, int minTasks, Duration probeEvery) {

    public Breaker {
        Objects.requireNonNull(probeEvery, "probeEvery");
        if (window < 0 || minTasks < 1) {
            throw new IllegalArgumentException("window must be at least 0 and minTasks at least 1");
        }
        if (window > 0 && (opensAt <= 0 || opensAt > 1)) {
            throw new IllegalArgumentException("opensAt must be above 0 and at most 1");
        }
        if (!probeEvery.isPositive()) {
            throw new IllegalArgumentException("probeEvery must be positive");
        }
    }

    /** Opens when 80 % of the last 50 results failed, from at least 5 tasks; probes every 30 s. */
    public static Breaker standard() {
        return new Breaker(0.8, 50, 5, Duration.ofSeconds(30));
    }

    /** Never opens: the topic claims at full speed whatever fails. */
    public static Breaker off() {
        return new Breaker(1, 0, 1, Duration.ofSeconds(30));
    }

    public boolean enabled() {
        return window > 0;
    }

    public Breaker opensAt(double share, int ofLast) {
        return new Breaker(share, ofLast, minTasks, probeEvery);
    }

    public Breaker probeEvery(Duration interval) {
        return new Breaker(opensAt, window, minTasks, interval);
    }

    public Breaker minTasks(int tasks) {
        return new Breaker(opensAt, window, tasks, probeEvery);
    }

    /** "opens at 80 % of the last 50 (5 tasks), probes every PT30S", for the effective settings. */
    public String describe() {
        return !enabled() ? "off"
            : "opens at " + Math.round(opensAt * 100) + " % of the last " + window + " (" + minTasks + " tasks), probes every " + probeEvery;
    }
}
