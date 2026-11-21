package io.github.khopland.task2.slice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.task2.BatchResource;
import io.github.khopland.task2.Decisions;
import io.github.khopland.task2.Task;
import io.github.khopland.task2.TopicRef;

/**
 * Stages feed publications while tasks run and writes them on the completion connection, only
 * for tasks whose final decision continues. A rollback of the completion removes them too.
 */
public final class FeedPublisher implements BatchResource<FeedPublisher.Staging> {

    public record Publication(Task<?> task, String person, String notice) {
    }

    public static final class Staging {

        private final List<Publication> staged = new CopyOnWriteArrayList<>();

        public void publish(Task<?> task, String person, String notice) {
            staged.add(new Publication(task, person, notice));
        }
    }

    /** Test hook: how many of the next completions fail while writing publications. */
    public final AtomicInteger failures = new AtomicInteger();

    @Override
    public Staging open(TopicRef<?> topic, List<Task<?>> tasks) {
        return new Staging();
    }

    @Override
    public void beforeCommit(Staging staging, Decisions finalDecisions, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO published_feed (person, notice, source_task) VALUES (?, ?, ?)")) {
            for (Publication publication : staging.staged) {
                if (finalDecisions.decision(publication.task()).continues()) {
                    statement.setString(1, publication.person());
                    statement.setString(2, publication.notice());
                    statement.setLong(3, publication.task().id());
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        }
        if (failures.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
            throw new SQLException("Simulated failure while publishing");
        }
    }

    @Override
    public String toString() {
        return "FeedPublisher";
    }
}
