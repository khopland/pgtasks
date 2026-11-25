package io.github.khopland.pgtasks.engine;

import java.sql.SQLException;

import io.github.khopland.pgtasks.TaskStorageException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TransientTest {

    @Test
    void a_lost_connection_a_deadlock_or_a_restart_is_transient() {
        assertThat(Transient.is(new SQLException("gone", "08006"))).isTrue();
        assertThat(Transient.is(new SQLException("deadlock", "40P01"))).isTrue();
        assertThat(Transient.is(new SQLException("shutting down", "57P01"))).isTrue();
        assertThat(Transient.is(new TaskStorageException("push", new SQLException("serialization", "40001")))).isTrue();
    }

    /**
     * Review 2026-11-25: a statement or lock timeout and a database that turned read-only during a
     * failover failed a batch's completion at once, every task in it counted as failed.
     */
    @Test
    void timeouts_resources_and_a_read_only_database_may_pass_but_are_not_transient() {
        for (String state : new String[] {"57014", "55P03", "53200", "53300", "25006"}) {
            assertThat(Transient.mayPass(new TaskStorageException("push", new SQLException("x", state)))).as(state).isTrue();
            assertThat(Transient.is(new SQLException("x", state))).as(state).isFalse();
        }
        assertThat(Transient.mayPass(new SQLException("unique", "23505"))).isFalse();
        assertThat(Transient.mayPass(new SQLException("raised", "P0001"))).isFalse();
    }
}
