package org.example.task2.spi;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** A clock that only moves when told to: for the in-memory store in tests and local runs. */
public final class ManualClock implements InstantSource {

    private final AtomicReference<Instant> now;

    public ManualClock(Instant start) {
        now = new AtomicReference<>(Objects.requireNonNull(start));
    }

    /** Starts at the current time. */
    public static ManualClock startingNow() {
        return new ManualClock(Instant.now());
    }

    @Override
    public Instant instant() {
        return now.get();
    }

    public Instant advance(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Time only moves forward");
        }
        return now.updateAndGet(current -> current.plus(duration));
    }
}
