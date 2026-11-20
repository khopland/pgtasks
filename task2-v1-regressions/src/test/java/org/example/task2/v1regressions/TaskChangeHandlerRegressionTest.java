package org.example.task2.v1regressions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.TaskProcessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc.DefaultTaskChangeHandler;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.processor.jdbc.TaskChangeEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Finding #2 (plan section 02): the switch in DefaultTaskChangeHandler falls through. */
class TaskChangeHandlerRegressionTest {

    @Test
    @DisplayName("#2 an activation starts the processor and leaves it running")
    void finding_2_activation_starts_processor() {
        FakeProcessor processor = new FakeProcessor(TaskProcessor.Activation.ACTIVE, false);
        AtomicReference<Throwable> error = new AtomicReference<>();

        new DefaultTaskChangeHandler(1, TimeUnit.SECONDS, error::set).accept(processor, TaskChangeEvent.ACTIVATION);

        assertThat(error.get()).as("error passed to onError").isNull();
        assertThat(processor.calls).containsExactly("start");
        assertThat(processor.isActive()).isTrue();
    }

    @Test
    @DisplayName("#2 a deactivation stops the processor without an error")
    void finding_2_deactivation_stops_processor() {
        FakeProcessor processor = new FakeProcessor(TaskProcessor.Activation.INACTIVE, true);
        AtomicReference<Throwable> error = new AtomicReference<>();

        new DefaultTaskChangeHandler(1, TimeUnit.SECONDS, error::set).accept(processor, TaskChangeEvent.ACTIVATION);

        assertThat(error.get()).as("error passed to onError").isNull();
        assertThat(processor.calls).containsExactly("stop");
        assertThat(processor.isActive()).isFalse();
    }

    private static final class FakeProcessor implements TaskProcessor {

        private final Activation activation;
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private volatile boolean active;

        FakeProcessor(Activation activation, boolean active) {
            this.activation = activation;
            this.active = active;
        }

        @Override
        public boolean start(long timeout, TimeUnit timeUnit) {
            calls.add("start");
            active = true;
            return true;
        }

        @Override
        public boolean stop(long timeout, TimeUnit timeUnit) {
            calls.add("stop");
            active = false;
            return true;
        }

        @Override
        public boolean resume() {
            return true;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public String getTopic() {
            return "topic";
        }

        @Override
        public Optional<Activation> findActivation() {
            return Optional.of(activation);
        }
    }
}
