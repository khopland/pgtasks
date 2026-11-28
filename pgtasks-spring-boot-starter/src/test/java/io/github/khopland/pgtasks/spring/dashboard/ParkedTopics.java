package io.github.khopland.pgtasks.spring.dashboard;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.OperatorView;

import org.jspecify.annotations.Nullable;

/**
 * A test extension shaped like a pause: a parked topic claims nothing. Its operator view lists the
 * parked topics with "Unpark" on each row, and offers "Park" or "Unpark" on every topic, so the
 * endpoint, the dashboard and the health have a view, row actions, topic actions and a held topic.
 */
public final class ParkedTopics implements Extension {

    private final Map<String, Instant> parked = new ConcurrentHashMap<>();

    @Override
    public String name() {
        return "parking";
    }

    public boolean isParked(String topic) {
        return parked.containsKey(topic);
    }

    @Override
    public void install(ExtensionContext context) {
        context.claimGate((topic, wanted) -> parked.containsKey(topic.name()) ? ClaimGate.Permit.waitFor(Duration.ofMinutes(1)) : ClaimGate.Permit.ANY);
        context.operatorView(new OperatorView() {
            @Override
            public String name() {
                return "parking";
            }

            @Override
            public String title() {
                return "Parked topics";
            }

            @Override
            public String note() {
                return "A parked topic claims nothing until it is unparked.";
            }

            @Override
            public List<Column> columns() {
                return List.of(Column.of("topic", "Topic", Column.Kind.TOPIC), Column.of("since", "Parked", Column.Kind.TIME),
                    Column.of("here", "Here", Column.Kind.FLAG));
            }

            @Override
            public List<Row> rows() {
                return parked.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .map(entry -> Row.of(entry.getKey(), Map.of("topic", entry.getKey(), "since", entry.getValue(), "here", true), "unpark"))
                    .toList();
            }

            @Override
            public @Nullable TopicState topic(String topic) {
                return parked.containsKey(topic) ? TopicState.of("parked", "unpark", "break") : TopicState.of(null, "park");
            }

            @Override
            public List<Action> actions() {
                return List.of(
                    Action.of("park", "Park", "No node claims from it until it is unparked.", topic -> parked.put(topic, context.clock().instant())),
                    Action.of("unpark", "Unpark", null, topic -> {
                        parked.remove(topic);
                        context.wake(topic);
                    }),
                    Action.of("break", "Break", null, topic -> {
                        throw new IllegalStateException("could not break " + topic);
                    }));
            }
        });
    }
}
