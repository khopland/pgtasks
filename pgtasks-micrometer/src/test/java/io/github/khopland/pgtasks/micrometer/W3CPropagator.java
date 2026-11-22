package io.github.khopland.pgtasks.micrometer;

import java.util.List;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;

/** Just enough of W3C trace context for {@code SimpleTracer}, which comes without a propagator. */
final class W3CPropagator implements Propagator {

    static final String TRACEPARENT = "traceparent";

    private final Tracer tracer;

    W3CPropagator(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public List<String> fields() {
        return List.of(TRACEPARENT);
    }

    @Override
    public <C> void inject(TraceContext context, C carrier, Setter<C> setter) {
        setter.set(carrier, TRACEPARENT, "00-" + context.traceId() + "-" + context.spanId() + "-01");
    }

    @Override
    public <C> Span.Builder extract(C carrier, Getter<C> getter) {
        String traceparent = getter.get(carrier, TRACEPARENT);
        if (traceparent == null) {
            return tracer.spanBuilder();
        }
        String[] parts = traceparent.split("-");
        TraceContext parent = tracer.traceContextBuilder().traceId(parts[1]).spanId(parts[2]).sampled(true).build();
        return tracer.spanBuilder().setParent(parent);
    }
}
