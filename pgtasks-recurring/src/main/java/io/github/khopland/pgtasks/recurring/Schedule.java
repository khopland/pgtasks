package io.github.khopland.pgtasks.recurring;

import java.util.List;
import java.util.Objects;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Pushed;
import io.github.khopland.pgtasks.TaskWriter;
import io.github.khopland.pgtasks.TopicRef;

/**
 * A task pushed on a cron schedule, declared to {@link Recurring}.
 *
 * <pre>
 * Schedule.of(REPORTS, "nightly", Cron.parse("0 2 * * *", ZoneId.of("Europe/Oslo")), new ReportRequest("daily"))
 * </pre>
 *
 * @param name       what tells it apart from the topic's other schedules, at most {@value #MAX_NAME_LENGTH}
 *                   characters; renaming one is removing it and adding another
 * @param identifier the pushed task's identifier, the same every run; the name unless set
 * @param payload    the pushed task's payload, the same every run
 * @param insertion  how each push treats an earlier run still waiting: {@link Insertion#SUPERSEDE}
 *                   unless set, so runs don't pile up while the topic is behind or its workers are down
 */
public record Schedule<T>(TopicRef<T> topic, String name, Cron cron, String identifier, T payload, Insertion insertion) {

    public static final int MAX_NAME_LENGTH = 200;

    public Schedule {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(cron, "cron");
        Objects.requireNonNull(identifier, "identifier");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(insertion, "insertion");
        if (name.isBlank() || name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("A schedule's name is 1 to " + MAX_NAME_LENGTH + " characters, not '" + name + "'");
        }
        // Checks the identifier as a push would, so a bad one fails the build rather than every run.
        Creation.of(identifier, payload);
    }

    /** Pushes {@code payload} with the schedule's name as its identifier, superseding a run still waiting. */
    public static <T> Schedule<T> of(TopicRef<T> topic, String name, Cron cron, T payload) {
        return new Schedule<>(topic, name, cron, name, payload, Insertion.SUPERSEDE);
    }

    public Schedule<T> withIdentifier(String identifier) {
        return new Schedule<>(topic, name, cron, identifier, payload, insertion);
    }

    /** {@link Insertion#APPEND} keeps every run, even when an earlier one hasn't run yet. */
    public Schedule<T> withInsertion(Insertion insertion) {
        return new Schedule<>(topic, name, cron, identifier, payload, insertion);
    }

    ScheduleStore.Key key() {
        return new ScheduleStore.Key(topic.name(), name);
    }

    List<Pushed> pushTo(TaskWriter writer) {
        return writer.push(topic, insertion, List.of(Creation.of(identifier, payload)));
    }

    @Override
    public String toString() {
        return name + " (" + topic.name() + ", " + cron + ")";
    }
}
