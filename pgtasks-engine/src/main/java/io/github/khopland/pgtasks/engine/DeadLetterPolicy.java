package io.github.khopland.pgtasks.engine;

import java.time.Duration;

/**
 * What happens to a topic's dead letters besides waiting for an operator.
 *
 * @param reviveOnNewVersion when the first node of a new application version starts, it revives
 *                           them once: a task that failed because of a bug runs again once the
 *                           fix is deployed. Needs the application's version, which the Spring
 *                           starter takes from {@code BuildProperties}; see
 *                           {@link TaskEngineBuilder#version(String)}
 * @param reviveEvery        revive them all this often, like the 1.x reset job; null for never
 */
public record DeadLetterPolicy(boolean reviveOnNewVersion, Duration reviveEvery) {

    /** Kept until someone revives or discards them. */
    public static final DeadLetterPolicy KEEP = new DeadLetterPolicy(false, null);

    public DeadLetterPolicy {
        if (reviveEvery != null && !reviveEvery.isPositive()) {
            throw new IllegalArgumentException("reviveEvery must be positive");
        }
    }

    public DeadLetterPolicy withReviveOnNewVersion() {
        return new DeadLetterPolicy(true, reviveEvery);
    }

    public DeadLetterPolicy withReviveEvery(Duration interval) {
        return new DeadLetterPolicy(reviveOnNewVersion, interval);
    }

    public String describe() {
        if (!reviveOnNewVersion && reviveEvery == null) {
            return "kept until revived or discarded";
        }
        return "revived" + (reviveOnNewVersion ? " on a new version" : "") + (reviveOnNewVersion && reviveEvery != null ? " and" : "")
            + (reviveEvery != null ? " every " + reviveEvery : "");
    }
}
