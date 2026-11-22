package io.github.khopland.pgtasks.micrometer;

import java.util.List;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;

/** Just enough of B3's single header for {@code SimpleTracer}, plus a baggage field, to show nothing depends on W3C. */
final class B3Propagator implements Propagator {

    static final String B3 = "b3";
    static final String BAGGAGE = "baggage-tenant";

    private final Tracer tracer;

    B3Propagator(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public List<String> fields() {
        return List.of(B3, BAGGAGE);
    }

    @Override
    public <C> void inject(TraceContext context, C carrier, Setter<C> setter) {
        setter.set(carrier, B3, context.traceId() + "-" + context.spanId() + "-1");
        setter.set(carrier, BAGGAGE, "skatt");
    }

    @Override
    public <C> Span.Builder extract(C carrier, Getter<C> getter) {
        String b3 = getter.get(carrier, B3);
        if (b3 == null) {
            return tracer.spanBuilder();
        }
        String[] parts = b3.split("-");
        TraceContext parent = tracer.traceContextBuilder().traceId(parts[0]).spanId(parts[1]).sampled(true).build();
        return tracer.spanBuilder().setParent(parent);
    }
}
