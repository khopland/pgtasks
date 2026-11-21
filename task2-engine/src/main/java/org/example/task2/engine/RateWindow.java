package org.example.task2.engine;

import java.util.Objects;

/**
 * A rate that a limit switches to each time {@code from} fires, until another of its windows
 * fires. See {@link GlobalOptions.LimitOptions#window}.
 */
public record RateWindow(Cron from, Rate rate) {

    public RateWindow {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(rate, "rate");
    }
}
