package io.github.khopland.task2.engine;

import java.util.Objects;

/**
 * A cluster-wide cap on how many tasks are claimed: one token bucket in the store, charged in the
 * claiming transaction, so over any interval t at most {@code burst + perSecond * t} tasks are
 * claimed across every node. Permits are taken before polling, and empty polls aren't charged.
 *
 * @param name      a limit can be shared by several topics, named after a topic or a downstream system
 * @param perSecond the rate until one of the windows has fired
 * @param windows   rates for times of day or week: each applies from when its cron fires until
 *                  another one fires; see {@link #at(java.time.Instant)}
 */
public record RateLimit(String name, double perSecond, int burst, java.util.List<RateWindow> windows) {

    public RateLimit {
        Objects.requireNonNull(name, "name");
        windows = java.util.List.copyOf(windows);
        if (!(perSecond > 0)) {
            throw new IllegalArgumentException("rate must be positive; a rate of 0 is not a pause");
        }
        if (burst < 1) {
            throw new IllegalArgumentException("burst must be at least 1");
        }
    }

    public static RateLimit perSecond(String name, double perSecond, int burst) {
        return new RateLimit(name, perSecond, burst, java.util.List.of());
    }

    public RateLimit withRate(double perSecond, int burst) {
        return new RateLimit(name, perSecond, burst, windows);
    }

    /**
     * The limit as it applies at a time: the rate of the window whose cron fired last, at or
     * before {@code now}; the base rate if there are no windows. With windows, give one for each
     * change, such as 50/s from 07:00 on weekdays and 200/s from 17:00, since each holds until the
     * next one fires. The windows stay on the result, so its rate is the only thing that changes.
     */
    public RateLimit at(java.time.Instant now) {
        RateWindow current = null;
        java.time.Instant since = null;
        for (RateWindow window : windows) {
            java.time.Instant fired = window.from().previous(now);
            if (fired != null && (since == null || !fired.isBefore(since))) {
                current = window;
                since = fired;
            }
        }
        return current == null ? this : withRate(current.rate().perSecond(), burst);
    }
}
