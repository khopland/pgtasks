package io.github.khopland.task2.slice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.github.khopland.task2.Creation;
import io.github.khopland.task2.Insertion;
import io.github.khopland.task2.Tasks;

/**
 * Reads a page of the upstream feed, enqueues one assessment per entry and advances its
 * pointer, all on the caller's connection, so they commit or roll back together.
 */
public final class IncomeFeed {

    private final Tasks tasks;

    public IncomeFeed(Tasks tasks) {
        this.tasks = tasks;
    }

    /** Returns how many entries were read. Does not commit. */
    public int readPage(Connection connection, int size) throws SQLException {
        long pointer;
        try (PreparedStatement statement = connection.prepareStatement("SELECT position FROM feed_pointer WHERE name = 'income' FOR UPDATE");
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            pointer = resultSet.getLong(1);
        }
        List<Creation<Slice.AssessmentRequest>> creations = new ArrayList<>();
        long last = pointer;
        try (PreparedStatement statement = connection.prepareStatement("SELECT position, person, income FROM incoming WHERE position > ? ORDER BY position LIMIT ?")) {
            statement.setLong(1, pointer);
            statement.setInt(2, size);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    last = resultSet.getLong(1);
                    String person = resultSet.getString(2);
                    creations.add(Creation.of(person, new Slice.AssessmentRequest(person, resultSet.getLong(3))).withReference("income:" + last));
                }
            }
        }
        if (creations.isEmpty()) {
            return 0;
        }
        // A newer income for the same person supersedes an assessment that hasn't run yet.
        tasks.on(connection).push(Slice.ASSESSMENT, Insertion.SUPERSEDE, creations);
        try (PreparedStatement statement = connection.prepareStatement("UPDATE feed_pointer SET position = ? WHERE name = 'income'")) {
            statement.setLong(1, last);
            statement.executeUpdate();
        }
        return creations.size();
    }
}
