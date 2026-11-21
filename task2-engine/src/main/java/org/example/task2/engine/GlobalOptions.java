package org.example.task2.engine;

import java.time.Duration;
import java.time.Period;
import java.util.function.Consumer;

import org.example.task2.Outcome;

/** Settings shared by every module. */
public interface GlobalOptions {

    GlobalOptions node(String name);

    GlobalOptions heartbeat(Duration interval, Duration expiry);

    /**
     * Node-wide cap on batches claiming or completing at once, each on a connection of its own.
     * By default half the pool's size when the pool says (HikariCP does), at most 8.
     */
    GlobalOptions maxInFlightBatches(int batches);

    /** Defaults for every topic, below each module's own defaults. */
    GlobalOptions defaults(Consumer<TopicOptions> defaults);

    /** A rate limit any topic can use, named after the topic or the downstream system it protects. */
    GlobalOptions limit(String name, Consumer<LimitOptions> limit);

    GlobalOptions housekeeping(Consumer<HousekeepingOptions> housekeeping);

    interface LimitOptions {

        LimitOptions rate(Rate rate);

        LimitOptions burst(int permits);

        /**
         * From each time {@code from} fires, the limit runs at {@code rate} until another window
         * fires: give one window for each change, as in
         * {@code .window(Cron.parse("0 7 * * MON-FRI", oslo), Rate.perSecond(50)).window(Cron.parse("0 17 * * MON-FRI", oslo), Rate.perSecond(200))}.
         * Operators' overrides still win over windows. Every node works out the same rate from
         * its clock, so the cluster-wide bucket sees one rate.
         */
        LimitOptions window(Cron from, Rate rate);

        /** A window whose cron is in UTC. */
        default LimitOptions window(String from, Rate rate) {
            return window(Cron.parse(from), rate);
        }

        /**
         * Splits the rate across live nodes instead of keeping one bucket in the database. Saves
         * a write per claim, but can overshoot while nodes join or leave. Strict is the default.
         *
         * <p>Not built yet: the limit is still enforced strictly. Deferred until someone needs
         * rates where one write per claimed batch is too much.
         */
        LimitOptions approximate();
    }

    interface HousekeepingOptions {

        /** How long finished tasks with this outcome are kept, counted from completion. */
        HousekeepingOptions retain(Outcome outcome, Period period);

        HousekeepingOptions retainForever(Outcome outcome);

        HousekeepingOptions promoteEvery(Duration interval);

        HousekeepingOptions snapshotCountsEvery(Duration interval);

        /** How often finished tasks past their retention are deleted. */
        HousekeepingOptions purgeEvery(Duration interval);

        /**
         * A task whose node dies under it waits {@code backoff} before it runs again, twice as
         * long the next time, and the {@code deadLetterAt}-th time it is dead-lettered, whatever
         * its retry policy. By default 30 s and the third time.
         */
        HousekeepingOptions expiries(int deadLetterAt, Duration backoff);
    }
}
