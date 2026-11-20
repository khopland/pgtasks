package org.example.task2.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SqlStatementsTest {

    @Test
    void splits_at_top_level_semicolons_only() {
        String sql = """
            -- header; with a semicolon
            CREATE TABLE a (v text DEFAULT 'x;y');
            /* block; /* nested; */ still comment; */
            INSERT INTO "odd;name" VALUES (E'it\\'s;fine', 'it''s;fine');
            CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $body$
            BEGIN
                RETURN 1; -- inside the body
            END
            $body$;
            DO $$ BEGIN PERFORM 1; END $$;
            -- trailing comment only;
            """;

        assertThat(SqlStatements.split(sql)).hasSize(4).satisfiesExactly(
            first -> assertThat(first).endsWith("CREATE TABLE a (v text DEFAULT 'x;y')"),
            second -> assertThat(second).endsWith("INSERT INTO \"odd;name\" VALUES (E'it\\'s;fine', 'it''s;fine')"),
            third -> assertThat(third).startsWith("CREATE FUNCTION f()").endsWith("$body$"),
            fourth -> assertThat(fourth).isEqualTo("DO $$ BEGIN PERFORM 1; END $$")
        );
    }

    @Test
    void positional_parameters_are_not_dollar_quotes() {
        assertThat(SqlStatements.split("SELECT $1; SELECT 2")).containsExactly("SELECT $1", "SELECT 2");
    }
}
