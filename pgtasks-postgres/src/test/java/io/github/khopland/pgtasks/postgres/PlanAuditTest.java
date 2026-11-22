package io.github.khopland.pgtasks.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/** The drained-queue pass would catch a statement that reads the ballast. */
class PlanAuditTest {

    @Test
    void counts_the_rows_each_node_on_the_queue_and_task_tables_examined() {
        String plan = """
            Nested Loop (actual rows=3 loops=1)
              Buffers: shared hit=500
              ->  Index Scan using queue_identifier_idx on queue b (actual rows=2 loops=10)
                    Index Cond: (topic_id = 1)
                    Rows Removed by Filter: 5
                    Buffers: shared hit=3
              ->  Seq Scan on topic (actual rows=1 loops=1)
                    Rows Removed by Filter: 1000
                    Buffers: shared hit=100
              ->  Bitmap Heap Scan on task t (actual rows=4 loops=1)
                    Rows Removed by Index Recheck: 6
                    Buffers: shared hit=5 read=2
                    ->  Bitmap Index Scan on task_completed_idx (actual rows=10 loops=1)
                          Buffers: shared hit=2
            """;

        assertThat(PlanAudit.usage(plan)).as("rows (2 + 5) * 10 + 4 + 6 + 10, buffers each node's own: 3 + (7 - 2) + 2; "
            + "the topic table doesn't count").isEqualTo(new PlanAudit.Usage(90, 10));
    }

    @Test
    void reads_the_average_rows_per_loop_that_postgres_18_shows_with_decimals() {
        String plan = """
            Nested Loop (actual rows=3.00 loops=1)
              Buffers: shared hit=40
              ->  Index Scan using queue_ready_idx on queue q (actual rows=0.50 loops=10)
                    Index Cond: (topic_id = 1)
                    Index Searches: 10
                    Rows Removed by Filter: 2
                    Buffers: shared hit=30
              ->  Index Scan using task_pkey on task t (actual rows=1.00 loops=5)
                    Buffers: shared hit=10
            """;

        assertThat(PlanAudit.usage(plan)).as("rows (0.5 + 2) * 10 + 1 * 5, buffers 30 + 10").isEqualTo(new PlanAudit.Usage(30, 40));
    }

    @Test
    void fails_on_a_scan_of_the_queue_it_cannot_read() {
        String plan = """
            Seq Scan on queue (actual rows=1,000 loops=1)
              Buffers: shared hit=30
            """;

        assertThatIllegalStateException().isThrownBy(() -> PlanAudit.usage(plan)).withMessageContaining("Seq Scan on queue");
    }

    @Test
    void flags_a_statement_that_reads_the_ballast() throws Exception {
        DrainedQueue drained = DrainedQueue.create(TaskSchema.DEFAULT_SCHEMA);
        try (drained; Connection connection = drained.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            // No index on lower(identifier): every plan reads the whole queue.
            assertThat(countIdentifier(connection, "lower(identifier)")).isEqualTo(10);
            connection.commit();
            assertThat(drained.audit().findings()).singleElement()
                .satisfies(finding -> assertThat(finding.usage().rows()).as("examined the ballast").isGreaterThan(DrainedQueue.ALLOWED.rows()));
        }
    }

    /**
     * Up to 17, an identifier without its topic can only walk all of queue_identifier_idx, which
     * reports a handful of rows and hundreds of buffers. 18's skip scan descends the index once per
     * topic instead (Index Searches: 3, about 20 buffers), so there it is cheap and not flagged.
     */
    @Test
    void flags_an_index_walk_over_the_ballast_before_postgres_18() throws Exception {
        DrainedQueue drained = DrainedQueue.create(TaskSchema.DEFAULT_SCHEMA);
        try (drained; Connection connection = drained.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            assertThat(countIdentifier(connection, "identifier")).isEqualTo(10);
            connection.commit();
            if (drained.database().count("SELECT current_setting('server_version_num')::integer / 10000") < 18) {
                assertThat(drained.audit().findings()).singleElement()
                    .satisfies(finding -> assertThat(finding.usage().buffers()).as("walked queue_identifier_idx over the ballast").isGreaterThan(300));
            } else {
                assertThat(drained.audit().audited()).isOne();
                assertThat(drained.audit().findings()).as("a skip scan").isEmpty();
            }
        }
    }

    private static long countIdentifier(Connection connection, String column) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM pgtasks.queue WHERE " + column + " = ?")) {
            statement.setString(1, "ballast-7");
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }
}
