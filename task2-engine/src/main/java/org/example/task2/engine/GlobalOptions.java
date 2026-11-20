package org.example.task2.engine;

import java.time.Duration;
import java.time.Period;
import java.util.function.Consumer;

import org.example.task2.Outcome;

/** Settings shared by every module. */
public interface GlobalOptions {

    GlobalOptions node(String name);

    GlobalOptions heartbeat(Duration interval, Duration expiry);

    /** Node-wide cap on batches in flight; keep it below the connection pool size. */
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
    }
}
