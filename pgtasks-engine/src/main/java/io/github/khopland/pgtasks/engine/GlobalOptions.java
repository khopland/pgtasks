package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Period;
import java.util.function.Consumer;

import io.github.khopland.pgtasks.Outcome;

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

    GlobalOptions housekeeping(Consumer<HousekeepingOptions> housekeeping);

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
