package org.example.task2.engine;

import java.time.Duration;
import java.time.Period;

import org.example.task2.BatchResource;
import org.example.task2.Ordering;
import org.example.task2.Outcome;

/** What differs from the defaults for a topic, or for a group of topics. */
public interface TopicOptions {

    TopicOptions batchSize(int tasks);

    /** Batches in flight per topic on one node. */
    TopicOptions concurrency(int batches);

    TopicOptions ordering(Ordering ordering);

    TopicOptions retry(Retry retry);

    TopicOptions batchDeadline(Duration deadline);

    TopicOptions pollInterval(Duration interval);

    /** Takes claim permits from a limit defined in {@link TaskGlobals}. */
    TopicOptions limit(String name);

    TopicOptions resources(BatchResource<?>... resources);

    /** How long finished tasks with this outcome are kept, counted from completion. */
    TopicOptions retain(Outcome outcome, Period period);

    TopicOptions retainForever(Outcome outcome);

    /** When the topic stops claiming because most of its recent tasks fail; {@link Breaker#off()} for never. */
    TopicOptions breaker(Breaker breaker);

    /** Whether dead letters are revived on a new application version, or every so often; kept until someone acts by default. */
    TopicOptions deadLetters(DeadLetterPolicy policy);
}
