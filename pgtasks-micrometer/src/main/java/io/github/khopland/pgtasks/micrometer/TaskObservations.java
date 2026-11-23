package io.github.khopland.pgtasks.micrometer;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskObserver;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.ReceiverContext;
import io.micrometer.observation.transport.SenderContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.MDC;

/**
 * The engine's observer for Micrometer.
 *
 * <ul>
 *   <li>An Observation {@value #PUSH} for every push, a producer: with tracing, every header the
 *       tracer's propagator writes ({@code traceparent} and {@code tracestate}, {@code baggage},
 *       {@code b3}, ...) is stored with the pushed tasks. Downstream work of a batch is pushed
 *       from the task that staged it.</li>
 *   <li>An Observation {@value #BATCH} for every batch, a consumer. If every task of the batch
 *       has the same headers, such as a batch of one or of tasks pushed together, it continues
 *       their trace; otherwise it starts a trace of its own.</li>
 *   <li>An Observation {@value #TASK} for each task the handler decides with
 *       {@code batch.decide}, {@code decideConcurrently} or {@code eachTask}, and for each task
 *       with downstream work, current while its decider runs and while its downstream work is
 *       written. It is a child of the batch if the batch continues the task's trace, and
 *       otherwise continues the trace the task was pushed from, so a batch of tasks from many
 *       traces continues each of them. Pushes made there are its children, so a trace follows
 *       the work from topic to topic.</li>
 *   <li>Counters, if a meter registry is given: {@value #PUSHED} by topic and status, and
 *       {@value #COMPLETED} by topic and result, both counted only once committed. The
 *       observations' own timers come from the registry's observation handlers.</li>
 *   <li>The logging MDC: {@value #MDC_TOPIC} while the batch runs and completes, and
 *       {@value #MDC_ID} and {@value #MDC_IDENTIFIER} for a batch of one, and while each task's
 *       own observation is current.</li>
 * </ul>
 *
 * The headers are written and read by the tracer's propagator, so any format it propagates
 * works: W3C, which is Micrometer Tracing's default with OpenTelemetry and Spring Boot's with
 * Brave, B3, and remote baggage fields.
 */
public final class TaskObservations implements TaskObserver {

    public static final String PUSH = "pgtasks.push";
    public static final String BATCH = "pgtasks.batch";
    public static final String TASK = "pgtasks.task";
    public static final String PUSHED = "pgtasks.pushed";
    public static final String COMPLETED = "pgtasks.completed";
    public static final String BREAKER = "pgtasks.breaker.open";
    public static final String MDC_TOPIC = "task.topic";
    public static final String MDC_ID = "task.id";
    public static final String MDC_IDENTIFIER = "task.identifier";

    private final ObservationRegistry observations;
    private final @Nullable MeterRegistry meters;
    private final Map<String, AtomicInteger> breakers = new ConcurrentHashMap<>();

    /** Observations and the MDC only, without counters. */
    public TaskObservations(ObservationRegistry observations) {
        this(observations, null);
    }

    /** @param meters for the counters, or null for none */
    public TaskObservations(ObservationRegistry observations, @Nullable MeterRegistry meters) {
        this.observations = Objects.requireNonNull(observations, "observations");
        this.meters = meters;
    }

    /** What a push observation sees: the topic and how many tasks, and the carrier the tracer writes its headers to. */
    public static final class PushContext extends SenderContext<Map<String, String>> {

        private final TopicRef<?> topic;
        private final int tasks;

        PushContext(TopicRef<?> topic, int tasks) {
            super((carrier, key, value) -> {
                if (carrier != null) {
                    carrier.put(key, value);
                }
            }, Kind.PRODUCER);
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

    /**
     * What a batch observation sees: the topic, the batch's tasks, and the headers it continues
     * the trace from, which are empty unless every task has the same.
     */
    public static final class BatchContext extends ReceiverContext<Map<String, String>> {

        private final TopicRef<?> topic;
        private final List<Task<?>> tasks;

        BatchContext(TopicRef<?> topic, List<Task<?>> tasks) {
            super(Map::get, Kind.CONSUMER);
            this.topic = topic;
            this.tasks = List.copyOf(tasks);
            setCarrier(shared(this.tasks));
            setRemoteServiceName("tasks");
        }

        public TopicRef<?> topic() {
            return topic;
        }

        public List<Task<?>> tasks() {
            return tasks;
        }

        /** Whether this batch continues the trace of the task: whether they have the same headers. */
        boolean continues(Task<?> task) {
            Map<String, String> carrier = getCarrier();
            return carrier != null && carrier.equals(task.headers());
        }

        private static Map<String, String> shared(List<Task<?>> tasks) {
            if (tasks.isEmpty()) {
                return Map.of();
            }
            Map<String, String> first = tasks.getFirst().headers();
            return tasks.stream().allMatch(task -> task.headers().equals(first)) ? first : Map.of();
        }
    }

    /** What a task observation sees when it is a child of the batch's: the task. */
    public static final class TaskContext extends Observation.Context {

        private final Task<?> task;

        TaskContext(Task<?> task) {
            this.task = task;
        }

        public Task<?> task() {
            return task;
        }
    }

    /**
     * What a task observation sees when it continues the trace the task was pushed from, because
     * the batch doesn't: the task, and its headers to continue the trace from.
     */
    public static final class TracedTaskContext extends ReceiverContext<Map<String, String>> {

        private final Task<?> task;

        TracedTaskContext(Task<?> task) {
            super(Map::get, Kind.CONSUMER);
            this.task = task;
            setCarrier(task.headers());
            setRemoteServiceName("tasks");
        }

        public Task<?> task() {
            return task;
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
            public Map<String, String> headers() {
                Map<String, String> carrier = context.getCarrier();
                return carrier == null ? Map.of() : Map.copyOf(carrier);
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
            AtomicInteger state = new AtomicInteger();
            Gauge.builder(BREAKER, state, AtomicInteger::get)
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
                Scope mdc = mdc(topic, single);
                return () -> {
                    mdc.close();
                    scope.close();
                };
            }

            @Override
            public TaskObservation task(Task<?> task) {
                Observation each = taskObservation(topic, task, context, observation);
                return new TaskObservation() {
                    @Override
                    public Scope open() {
                        Observation.Scope scope = each.openScope();
                        Scope mdc = mdc(topic, task);
                        return () -> {
                            mdc.close();
                            scope.close();
                        };
                    }

                    @Override
                    public void error(Throwable error) {
                        each.error(error);
                    }

                    @Override
                    public void close() {
                        each.stop();
                    }
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

    /**
     * A task's own observation: a child of the batch if the batch continues the task's trace, or
     * if the task has no headers to continue one from; otherwise it continues the task's trace.
     */
    private Observation taskObservation(TopicRef<?> topic, Task<?> task, BatchContext batch, Observation parent) {
        boolean ownTrace = !batch.continues(task) && !task.headers().isEmpty();
        Observation.Context context = ownTrace ? new TracedTaskContext(task) : new TaskContext(task);
        Observation observation = Observation.createNotStarted(TASK, () -> context, observations)
            .contextualName("handle " + topic.name())
            .lowCardinalityKeyValue("task.topic", topic.name())
            .highCardinalityKeyValue("task.id", Long.toString(task.id()))
            .highCardinalityKeyValue("task.identifier", task.identifier());
        if (!ownTrace) {
            observation.parentObservation(parent);
        }
        return observation.start();
    }

    /** The MDC for a batch, and for a task if there is one, until closed. */
    private static Scope mdc(TopicRef<?> topic, @Nullable Task<?> task) {
        Map<String, @Nullable String> previous = new HashMap<>();
        put(previous, MDC_TOPIC, topic.name());
        if (task != null) {
            put(previous, MDC_ID, Long.toString(task.id()));
            put(previous, MDC_IDENTIFIER, task.identifier());
        }
        return () -> previous.forEach((key, value) -> {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }

    private static void put(Map<String, @Nullable String> previous, String key, String value) {
        previous.put(key, MDC.get(key));
        MDC.put(key, value);
    }

    private Counter counter(String name, String description, TopicRef<?> topic, String tag, String value) {
        return Counter.builder(name).description(description).baseUnit("tasks")
            .tag("topic", topic.name()).tag(tag, value.toLowerCase(Locale.ROOT))
            .register(Objects.requireNonNull(meters, "meters"));
    }

    private static String name(Result result) {
        return result.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
}
