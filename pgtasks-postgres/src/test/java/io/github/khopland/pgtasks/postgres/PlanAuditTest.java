package io.github.khopland.pgtasks.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.jupiter.api.Test;

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
    void flags_a_statement_that_reads_the_ballast() throws Exception {
        DrainedQueue drained = DrainedQueue.create();
        try (drained; Connection connection = drained.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            // No topic: it can't use queue_identifier_idx to find one identifier's tasks.
            try (PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM tasks.queue WHERE identifier = ?")) {
                statement.setString(1, "ballast-7");
                try (ResultSet resultSet = statement.executeQuery()) {
                    resultSet.next();
                    assertThat(resultSet.getLong(1)).isEqualTo(10);
                }
            }
            connection.commit();
            assertThat(drained.audit().findings()).singleElement()
                .satisfies(finding -> assertThat(finding.usage().buffers()).as("walked queue_identifier_idx over the ballast").isGreaterThan(300));
        }
    }
}
