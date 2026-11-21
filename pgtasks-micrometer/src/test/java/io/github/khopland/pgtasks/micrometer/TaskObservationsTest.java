package io.github.khopland.pgtasks.micrometer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

class TaskObservationsTest {

    private static final TopicRef<String> ORDERS = TopicRef.of("orders", String.class);
    private static final TopicRef<String> RECEIPTS = TopicRef.of("receipts", String.class);

    record Seen(String traceId, String topic, String id, String identifier) {
    }

    private final SimpleTracer tracer = new SimpleTracer();
    private final MeterRegistry meters = new SimpleMeterRegistry();
    private final List<Seen> seen = new CopyOnWriteArrayList<>();
    private final List<Map<String, String>> headers = new CopyOnWriteArrayList<>();
    private ObservationRegistry observations;
    private TaskEngine engine;

    @BeforeEach
    void observe() {
        observe(new W3CPropagator(tracer));
    }

    private void observe(Propagator propagator) {
        observations = ObservationRegistry.create();
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

    /** Orders push a receipt each; built and prepared, so pushes are stored, but not started. */
    private TaskEngine build(int batchSize) {
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
                    headers.add(task.task().headers());
                    return task.payload().contains("bad") ? Decision.failed("bad receipt") : Decision.success();
                }), topic -> topic.batchSize(batchSize)))
            .build()
            .prepare();
        return engine;
    }

    private void record(String identifier) {
        SimpleSpan span = tracer.currentSpan();
        seen.add(new Seen(span == null ? null : span.getTraceId(), MDC.get(TaskObservations.MDC_TOPIC), MDC.get(TaskObservations.MDC_ID), identifier));
    }

    /** Pushes an order from a request of its own, and returns the request's trace id. */
    private String pushFromRequest(String identifier) {
        Observation request = Observation.createNotStarted("request", observations).contextualName("request " + identifier).start();
        try (Observation.Scope ignored = request.openScope()) {
            engine.tasks().independently().push(ORDERS, identifier, "book");
        } finally {
            request.stop();
        }
        return span("request " + identifier).getTraceId();
    }

    private SimpleSpan span(String name) {
        return tracer.getSpans().stream().filter(span -> name.equals(span.getName())).findFirst().orElseThrow();
    }

    private List<SimpleSpan> spans(String name) {
        return tracer.getSpans().stream().filter(span -> name.equals(span.getName())).toList();
    }

    @Test
    @DisplayName("A trace continues from the pushing request through every topic it reaches")
    void a_trace_follows_the_work_from_topic_to_topic() throws Exception {
        build(1).start();
        String traceId = pushFromRequest("order-1");
        TaskTester.of(engine).awaitIdle();

        assertThat(seen).extracting(Seen::traceId).containsExactly(traceId, traceId);
        assertThat(seen).extracting(Seen::topic).containsExactly("orders", "receipts");
        assertThat(seen).extracting(Seen::id).doesNotContainNull();

        // request -> push orders -> process orders -> handle orders -> push receipts -> process receipts -> handle receipts
        Map<String, SimpleSpan> spans = new HashMap<>();
        tracer.getSpans().forEach(span -> spans.put(span.getName(), span));
        assertThat(spans).containsKeys("push orders", "process orders", "handle orders", "push receipts", "process receipts", "handle receipts");
        assertThat(spans.get("push orders").getParentId()).isEqualTo(spans.get("request order-1").getSpanId());
        assertThat(spans.get("process orders").getParentId()).isEqualTo(spans.get("push orders").getSpanId());
        assertThat(spans.get("handle orders").getParentId()).isEqualTo(spans.get("process orders").getSpanId());
        assertThat(spans.get("push receipts").getParentId()).isEqualTo(spans.get("handle orders").getSpanId());
        assertThat(spans.get("process receipts").getParentId()).isEqualTo(spans.get("push receipts").getSpanId());
        assertThat(spans.get("handle receipts").getParentId()).isEqualTo(spans.get("process receipts").getSpanId());
        assertThat(spans.values()).extracting(SimpleSpan::getTraceId).containsOnly(traceId);
        assertThat(MDC.get(TaskObservations.MDC_TOPIC)).isNull();
    }

    @Test
    @DisplayName("A batch of tasks from several traces continues each one, and their downstream work too")
    void a_batch_of_tasks_from_several_traces_continues_each_of_them() throws Exception {
        build(10);
        Map<String, String> traces = new HashMap<>();
        for (String order : List.of("a", "b", "c")) {
            traces.put(order, pushFromRequest(order));
        }
        engine.start();
        TaskTester.of(engine).awaitIdle();

        assertThat(spans("process orders")).as("the three orders ran as one batch").hasSize(1);
        assertThat(traces.values()).doesNotHaveDuplicates();
        assertThat(seen).hasSize(6).allSatisfy(each -> assertThat(each.traceId()).isEqualTo(traces.get(each.identifier())));
        assertThat(seen).extracting(Seen::id).as("each task's own id in the MDC, in a batch of three").doesNotContainNull();
        for (SimpleSpan handle : spans("handle orders")) {
            SimpleSpan push = tracer.getSpans().stream().filter(span -> span.getSpanId().equals(handle.getParentId())).findFirst().orElseThrow();
            assertThat(push.getName()).isEqualTo("push orders");
            assertThat(push.getTraceId()).isEqualTo(handle.getTraceId());
        }
        assertThat(spans("push receipts")).hasSize(3).extracting(SimpleSpan::getTraceId).containsExactlyInAnyOrderElementsOf(traces.values());
        assertThat(span("process orders").getTraceId()).as("the batch is a trace of its own").isNotIn(traces.values());
    }

    @Test
    @DisplayName("Every header the propagator writes is stored, so a trace continues in any format")
    void every_propagated_header_is_stored() throws Exception {
        observe(new B3Propagator(tracer));
        build(1).start();
        String traceId = pushFromRequest("order-1");
        TaskTester.of(engine).awaitIdle();

        assertThat(seen).extracting(Seen::traceId).containsExactly(traceId, traceId);
        assertThat(headers).singleElement().satisfies(stored -> {
            assertThat(stored).containsOnlyKeys(B3Propagator.B3, B3Propagator.BAGGAGE);
            assertThat(stored.get(B3Propagator.B3)).startsWith(traceId + "-" + span("push receipts").getSpanId());
        });
    }

    @Test
    @DisplayName("Counters count committed pushes and results; the observations time pushes, batches and tasks")
    void counters_count_committed_pushes_and_results() throws Exception {
        build(10).start();
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
        assertThat(meters.get(TaskObservations.TASK).tags("task.topic", "orders").timer().count()).isEqualTo(2);
    }

    @Test
    @DisplayName("Without a tracer, nothing is stored and tasks still run")
    void without_a_tracer_tasks_run_and_store_no_headers() throws Exception {
        engine = TaskEngine.builder()
            .store(new InMemoryTaskStore())
            .observer(new TaskObservations(ObservationRegistry.NOOP))
            .configure("shop", tasks -> tasks.topic(ORDERS, batch -> batch.decide(task -> {
                headers.add(task.task().headers());
                seen.add(new Seen(null, MDC.get(TaskObservations.MDC_TOPIC), MDC.get(TaskObservations.MDC_ID), task.identifier()));
                return Decision.success();
            })))
            .build()
            .start();
        engine.tasks().independently().push(ORDERS, "x", "y");
        TaskTester.of(engine).awaitIdle();

        assertThat(headers).containsExactly(Map.of());
        assertThat(seen).singleElement().satisfies(each -> {
            assertThat(each.topic()).isEqualTo("orders");
            assertThat(each.id()).isNotNull();
        });
    }
}
