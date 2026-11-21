package org.example.task2.micrometer;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.ReceiverContext;
import io.micrometer.observation.transport.SenderContext;
import org.example.task2.Pushed;
import org.example.task2.Task;
import org.example.task2.TopicRef;
import org.example.task2.engine.TaskObserver;
import org.slf4j.MDC;

/**
 * The engine's observer for Micrometer.
 *
 * <ul>
 *   <li>An Observation {@value #PUSH} for every push, a producer: with tracing, its span's
 *       {@code traceparent} is stored with the pushed tasks.</li>
 *   <li>An Observation {@value #BATCH} for every batch, a consumer that continues the trace
 *       stored with the batch's first task that has one. A batch of one task (or of tasks pushed
 *       together) continues exactly its trace; a larger batch of tasks from different traces
 *       continues the first one's. Pushes made while handling or completing the batch are its
 *       children, so a trace follows the work from topic to topic.</li>
 *   <li>Counters, if a meter registry is given: {@value #PUSHED} by topic and status, and
 *       {@value #COMPLETED} by topic and result, both counted only once committed. The
 *       observations' own timers come from the registry's observation handlers.</li>
 *   <li>The logging MDC, while the batch runs and completes: {@value #MDC_TOPIC}, and for a
 *       batch of one task {@value #MDC_ID} and {@value #MDC_IDENTIFIER}.</li>
 * </ul>
 *
 * The trace is stored as a W3C {@code traceparent}, so it needs a tracer that propagates W3C
 * headers, which is Micrometer Tracing's default with OpenTelemetry and Spring Boot's with Brave.
 */
public final class TaskObservations implements TaskObserver {

    public static final String PUSH = "task.push";
    public static final String BATCH = "task.batch";
    public static final String PUSHED = "task.pushed";
    public static final String COMPLETED = "task.completed";
    public static final String BREAKER = "task.breaker.open";
    public static final String MDC_TOPIC = "task.topic";
    public static final String MDC_ID = "task.id";
    public static final String MDC_IDENTIFIER = "task.identifier";
    static final String TRACEPARENT = "traceparent";

    private final ObservationRegistry observations;
    private final MeterRegistry meters;
    private final Map<String, java.util.concurrent.atomic.AtomicInteger> breakers = new java.util.concurrent.ConcurrentHashMap<>();

    /** Observations and the MDC only, without counters. */
    public TaskObservations(ObservationRegistry observations) {
        this(observations, null);
    }

    /** @param meters for the counters, or null for none */
    public TaskObservations(ObservationRegistry observations, MeterRegistry meters) {
        this.observations = Objects.requireNonNull(observations, "observations");
        this.meters = meters;
    }

    /** What a push observation sees: the topic and how many tasks, and the carrier the tracer writes its headers to. */
    public static final class PushContext extends SenderContext<Map<String, String>> {

        private final TopicRef<?> topic;
        private final int tasks;

        PushContext(TopicRef<?> topic, int tasks) {
            super(Map::put, Kind.PRODUCER);
            this.topic = topic;
            this.tasks = tasks;
            setCarrier(new HashMap<>());
            setRemoteServiceName("tasks");
        }

        public TopicRef<?> topic() {
            return topic;
        }

        public int tasks() {
            return tasks;
        }
    }

    /** What a batch observation sees: the topic, the batch's tasks, and the stored trace it continues. */
    public static final class BatchContext extends ReceiverContext<Map<String, String>> {

        private final TopicRef<?> topic;
        private final List<Task<?>> tasks;

        BatchContext(TopicRef<?> topic, List<Task<?>> tasks) {
            super(Map::get, Kind.CONSUMER);
            this.topic = topic;
            this.tasks = List.copyOf(tasks);
            Map<String, String> carrier = new HashMap<>();
            this.tasks.stream().map(Task::traceContext).filter(Objects::nonNull).findFirst()
                .ifPresent(traceparent -> carrier.put(TRACEPARENT, traceparent));
            setCarrier(carrier);
            setRemoteServiceName("tasks");
        }

        public TopicRef<?> topic() {
            return topic;
        }

        public List<Task<?>> tasks() {
            return tasks;
        }
    }

    @Override
    public PushObservation pushing(TopicRef<?> topic, int tasks) {
        PushContext context = new PushContext(topic, tasks);
        Observation observation = Observation.createNotStarted(PUSH, () -> context, observations)
            .contextualName("push " + topic.name())
            .lowCardinalityKeyValue("task.topic", topic.name())
            .highCardinalityKeyValue("task.count", Integer.toString(tasks))
            .start();
        return new PushObservation() {
            @Override
            public String traceContext() {
                return context.getCarrier().get(TRACEPARENT);
            }

            @Override
            public void close() {
                observation.stop();
            }
        };
    }

    /** {@value #BREAKER}: 1 while this node's breaker for the topic is open, from the first time it opens. */
    @Override
    public void breaker(TopicRef<?> topic, boolean open) {
        if (meters == null) {
            return;
        }
        breakers.computeIfAbsent(topic.name(), name -> {
            java.util.concurrent.atomic.AtomicInteger state = new java.util.concurrent.atomic.AtomicInteger();
            io.micrometer.core.instrument.Gauge.builder(BREAKER, state, java.util.concurrent.atomic.AtomicInteger::get)
                .description("1 while this node has stopped claiming from the topic because most recent tasks failed")
                .tag("topic", name)
                .register(meters);
            return state;
        }).set(open ? 1 : 0);
    }

    @Override
    public void pushed(TopicRef<?> topic, List<Pushed> results) {
        if (meters == null) {
            return;
        }
        for (Pushed result : results) {
            counter(PUSHED, "Tasks pushed and committed", topic, "status", result.status().name()).increment();
        }
    }

    @Override
    public BatchObservation batch(TopicRef<?> topic, List<Task<?>> tasks) {
        BatchContext context = new BatchContext(topic, tasks);
        Observation observation = Observation.createNotStarted(BATCH, () -> context, observations)
            .contextualName("process " + topic.name())
            .lowCardinalityKeyValue("task.topic", topic.name())
            .highCardinalityKeyValue("task.count", Integer.toString(tasks.size()))
            .start();
        Task<?> single = tasks.size() == 1 ? tasks.getFirst() : null;
        return new BatchObservation() {
            private String result = "released";

            @Override
            public Scope open() {
                Observation.Scope scope = observation.openScope();
                Map<String, String> previous = new HashMap<>();
                put(previous, MDC_TOPIC, topic.name());
                if (single != null) {
                    put(previous, MDC_ID, Long.toString(single.id()));
                    put(previous, MDC_IDENTIFIER, single.identifier());
                }
                return () -> {
                    previous.forEach((key, value) -> {
                        if (value == null) {
                            MDC.remove(key);
                        } else {
                            MDC.put(key, value);
                        }
                    });
                    scope.close();
                };
            }

            @Override
            public void error(Throwable error) {
                observation.error(error);
            }

            @Override
            public void completed(Map<Result, Integer> results) {
                result = "completed";
                if (meters != null) {
                    results.forEach((outcome, count) ->
                        counter(COMPLETED, "Tasks whose batch completed, by what became of them", topic, "result", name(outcome)).increment(count));
                }
            }

            @Override
            public void close() {
                observation.lowCardinalityKeyValue("task.result", result);
                observation.stop();
            }
        };
    }

    private static void put(Map<String, String> previous, String key, String value) {
        previous.put(key, MDC.get(key));
        MDC.put(key, value);
    }

    private Counter counter(String name, String description, TopicRef<?> topic, String tag, String value) {
        return Counter.builder(name).description(description).baseUnit("tasks")
            .tag("topic", topic.name()).tag(tag, value.toLowerCase(Locale.ROOT))
            .register(meters);
    }

    private static String name(Result result) {
        return result.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
}
