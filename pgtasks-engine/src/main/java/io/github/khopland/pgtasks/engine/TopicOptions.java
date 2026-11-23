package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Period;

import io.github.khopland.pgtasks.BatchResource;
import io.github.khopland.pgtasks.Ordering;
import io.github.khopland.pgtasks.Outcome;

/** What differs from the defaults for a topic, or for a group of topics. */
public interface TopicOptions {

    TopicOptions batchSize(int tasks);

    /** Batches in flight per topic on one node. */
    TopicOptions concurrency(int batches);

    TopicOptions ordering(Ordering ordering);

    TopicOptions retry(Retry retry);

    TopicOptions batchDeadline(Duration deadline);

    TopicOptions pollInterval(Duration interval);

    TopicOptions resources(BatchResource<?>... resources);

    /** How long finished tasks with this outcome are kept, counted from completion. */
    TopicOptions retain(Outcome outcome, Period period);

    TopicOptions retainForever(Outcome outcome);
}
