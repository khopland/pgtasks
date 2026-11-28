package io.github.khopland.pgtasks.engine;

import java.util.List;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;

/** A claim gate with the name of the extension that registered it, for health, logs and metrics. */
record NamedGate(String name, ClaimGate gate) implements ClaimGate {

    /** The gate's name: its extension's, or its class's for a gate registered some other way. */
    static String nameOf(ClaimGate gate) {
        if (gate instanceof NamedGate named) {
            return named.name();
        }
        String simple = gate.getClass().getSimpleName();
        return simple.isEmpty() ? gate.getClass().getName() : simple;
    }

    @Override
    public Permit permit(TopicRef<?> topic, int wanted) {
        return gate.permit(topic, wanted);
    }

    @Override
    public void claimed(TopicRef<?> topic, List<Long> taskIds) {
        gate.claimed(topic, taskIds);
    }

    @Override
    public String toString() {
        return name;
    }
}
