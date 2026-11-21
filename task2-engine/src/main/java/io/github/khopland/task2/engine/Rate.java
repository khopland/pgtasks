package io.github.khopland.task2.engine;

import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A rate of task claims. Zero is not a rate: pausing a topic is a separate switch. */
public record Rate(double perSecond) {

    private static final Pattern TEXT = Pattern.compile("\\s*([0-9]+(?:\\.[0-9]+)?)\\s*/\\s*(s|sec|second|m|min|minute|h|hour)\\s*");

    public Rate {
        if (!(perSecond > 0) || Double.isInfinite(perSecond)) {
            throw new IllegalArgumentException("A rate must be positive; use pause to stop a topic");
        }
    }

    public static Rate perSecond(double permits) {
        return new Rate(permits);
    }

    public static Rate perMinute(double permits) {
        return new Rate(permits / 60);
    }

    public static Rate per(double permits, Duration period) {
        return new Rate(permits / (period.toNanos() / 1e9));
    }

    /** Parses "400/s", "100/min" or "5000/h". */
    public static Rate parse(String text) {
        Matcher matcher = TEXT.matcher(text.toLowerCase(Locale.ROOT));
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a rate: '" + text + "'; write it like 400/s, 100/min or 5000/h");
        }
        double permits = Double.parseDouble(matcher.group(1));
        return switch (matcher.group(2).charAt(0)) {
            case 's' -> perSecond(permits);
            case 'm' -> perMinute(permits);
            default -> per(permits, Duration.ofHours(1));
        };
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s/s", perSecond % 1 == 0 ? String.valueOf((long) perSecond) : String.valueOf(perSecond));
    }
}
