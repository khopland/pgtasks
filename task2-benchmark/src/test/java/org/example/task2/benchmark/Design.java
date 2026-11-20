package org.example.task2.benchmark;

import java.sql.SQLException;
import java.util.List;

import org.example.task2.Ordering;

/** A storage design under test: bulk setup in SQL, then claim and complete as workers do. */
interface Design {

    record Claimed(long id, int attempt) {
    }

    String name();

    void create() throws Exception;

    /** Finished tasks in the topic, spread over 200,000 identifiers, created over the past H seconds. */
    void history(long rows) throws SQLException;

    /** Dead-lettered tasks with distinct identifiers. */
    void deadLetters(int rows) throws SQLException;

    /** Ready tasks; task i gets identifier prefix + (i % identifiers). */
    void ready(int rows, String prefix, int identifiers) throws SQLException;

    List<Claimed> claim(int node, Ordering ordering, int limit, boolean admission) throws SQLException;

    void complete(int node, List<Claimed> claimed) throws SQLException;

    /** Bytes used by the design's tables and indexes. */
    long bytes() throws SQLException;

    void vacuum() throws SQLException;
}
