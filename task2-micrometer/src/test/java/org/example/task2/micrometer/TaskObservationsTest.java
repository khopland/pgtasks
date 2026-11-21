package org.example.task2.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
import org.example.task2.Decision;
import org.example.task2.TopicRef;
import org.example.task2.engine.TaskEngine;
import org.example.task2.engine.TaskTester;
import org.example.task2.memory.InMemoryTaskStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class TaskObservationsTest {

    private static final TopicRef<String> ORDERS = TopicRef.of("orders", String.class);
    private static final TopicRef<String> RECEIPTS = TopicRef.of("receipts", String.class);

    record Seen(String traceId, String topic, String id, String identifier) {
    }

    private final SimpleTracer tracer = new SimpleTracer();
    private final MeterRegistry meters = new SimpleMeterRegistry();
    private final ObservationRegistry observations = ObservationRegistry.create();
    private final List<Seen> seen = new CopyOnWriteArrayList<>();
    private TaskEngine engine;

    @BeforeEach
    void observe() {
        W3CPropagator propagator = new W3CPropagator(tracer);
        observations.observationConfig()
            .observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
                new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
                new DefaultTracingObservationHandler(tracer)))
            .observationHandler(new DefaultMeterObservationHandler(meters));
    }

    @AfterEach
    void stop() {
        if (engine != null) {
            engine.close();
        }
    }

    private TaskEngine start(int batchSize) throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .observer(new TaskObservations(observations, meters))
            .configure("shop", tasks -> tasks
                .topic(ORDERS, batch -> batch.decide(task -> {
                    record(task.identifier());
                    task.push(RECEIPTS, task.identifier(), "receipt for " + task.payload());
                    return Decision.success();
                }), topic -> topic.batchSize(batchSize))
                .topic(RECEIPTS, batch -> batch.decide(task -> {
                    record(task.identifier());
                    return task.payload().contains("bad") ? Decision.failed("bad receipt") : Decision.success();
                })))
            .build()
            .start();
        return engine;
    }

    private void record(String identifier) {
        SimpleSpan span = tracer.currentSpan();
        seen.add(new Seen(span == null ? null : span.getTraceId(), MDC.get(TaskObservations.MDC_TOPIC), MDC.get(TaskObservations.MDC_ID), identifier));
    }

    @Test
    @DisplayName("A trace continues from the pushing request through every topic it reaches")
    void a_trace_follows_the_work_from_topic_to_topic() throws Exception {
        start(1);
        Observation request = Observation.start("request", observations);
        try (Observation.Scope ignored = request.openScope()) {
            engine.tasks().independently().push(ORDERS, "order-1", "book");
        } finally {
            request.stop();
        }
        TaskTester.of(engine).awaitIdle();

        String traceId = tracer.getSpans().stream().filter(span -> span.getName().equals("request")).findFirst().orElseThrow().getTraceId();
        assertThat(seen).extracting(Seen::traceId).containsExactly(traceId, traceId);
        assertThat(seen).extracting(Seen::topic).containsExactly("orders", "receipts");
        assertThat(seen).extracting(Seen::id).doesNotContainNull();

        // request -> push orders -> process orders -> push receipts -> process receipts
        Map<String, SimpleSpan> spans = new java.util.HashMap<>();
        tracer.getSpans().forEach(span -> spans.put(span.getName(), span));
        assertThat(spans).containsKeys("push orders", "process orders", "push receipts", "process receipts");
        assertThat(spans.get("push orders").getParentId()).isEqualTo(spans.get("request").getSpanId());
        assertThat(spans.get("process orders").getParentId()).isEqualTo(spans.get("push orders").getSpanId());
        assertThat(spans.get("push receipts").getParentId()).isEqualTo(spans.get("process orders").getSpanId());
        assertThat(spans.get("process receipts").getParentId()).isEqualTo(spans.get("push receipts").getSpanId());
        assertThat(spans.values()).extracting(SimpleSpan::getTraceId).containsOnly(traceId);
        assertThat(MDC.get(TaskObservations.MDC_TOPIC)).isNull();
    }

    @Test
    @DisplayName("Counters count committed pushes and results; the observations time pushes and batches")
    void counters_count_committed_pushes_and_results() throws Exception {
        start(10);
        engine.tasks().independently().push(ORDERS, "good", "fine");
        engine.tasks().independently().push(ORDERS, "worse", "bad");
        TaskTester.of(engine).awaitIdle();

        assertThat(meters.get(TaskObservations.PUSHED).tags("topic", "orders", "status", "queued").counter().count()).isEqualTo(2);
        assertThat(meters.get(TaskObservations.PUSHED).tags("topic", "receipts", "status", "queued").counter().count()).isEqualTo(2);
        assertThat(meters.get(TaskObservations.COMPLETED).tags("topic", "orders", "result", "succeeded").counter().count()).isEqualTo(2);
        assertThat(meters.get(TaskObservations.COMPLETED).tags("topic", "receipts", "result", "succeeded").counter().count()).isEqualTo(1);
        assertThat(meters.get(TaskObservations.COMPLETED).tags("topic", "receipts", "result", "retrying").counter().count()).isEqualTo(1);
        assertThat(meters.get(TaskObservations.BATCH).tags("task.topic", "orders", "task.result", "completed").timer().count()).isPositive();
        assertThat(meters.get(TaskObservations.PUSH).tags("task.topic", "receipts").timer().count()).isPositive();
    }

    @Test
    @DisplayName("Without a tracer, nothing is stored and tasks still run")
    void without_a_tracer_tasks_run_and_store_no_trace() throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .observer(new TaskObservations(ObservationRegistry.NOOP))
            .configure("shop", tasks -> tasks.topic(ORDERS, batch -> batch.decide(task -> {
                seen.add(new Seen(task.task().traceContext(), MDC.get(TaskObservations.MDC_TOPIC), null, task.identifier()));
                return Decision.success();
            })))
            .build()
            .start();
        engine.tasks().independently().push(ORDERS, "x", "y");
        TaskTester.of(engine).awaitIdle();

        assertThat(seen).containsExactly(new Seen(null, "orders", null, "x"));
    }
}
