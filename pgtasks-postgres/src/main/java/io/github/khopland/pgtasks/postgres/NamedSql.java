package io.github.khopland.pgtasks.postgres;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.postgresql.PGStatement;

/**
 * SQL with {@code :name} parameters, so one value can be bound in several places.
 *
 * <p>Every statement is planned for its actual parameters on every execution, never from a cached
 * generic plan. The driver prepares a statement on the server after a few executions, and Postgres
 * may then keep one generic plan for the connection's lifetime. Made while the tables were small or
 * their statistics said empty, which is how every install starts and how the queue looks after each
 * burst drains, it keeps that shape as they grow, and nothing invalidates it until the next analyze:
 * claims were 30 times slower after a topic sat idle (Phase 2 benchmark), and a completion read its
 * topic's whole history (NOTIFY benchmark, CompletionPlanTest). A plan made per execution sees the
 * tables' current size. Planning costs about 0.1 ms a statement, so a statement that is run very
 * often and can't be misplanned may opt out ({@link #withGenericPlans()}).
 */
final class NamedSql {

    private static final Pattern PARAMETER = Pattern.compile("(?<![:\\w]):([a-z_]+)");

    private final String sql;
    private final List<String> names;
    private final boolean genericPlans;

    private NamedSql(String sql, List<String> names, boolean genericPlans) {
        this.sql = sql;
        this.names = names;
        this.genericPlans = genericPlans;
    }

    /**
     * The same statement, allowed a cached generic plan: only for one that reads no table by a
     * condition whose selectivity the statistics could get wrong, such as an insert. PlanAudit explains
     * such a statement's generic plan as made the first time it ran, in the TCK and CompletionPlanTest.
     */
    NamedSql withGenericPlans() {
        return new NamedSql(sql, names, true);
    }

    static NamedSql parse(String text) {
        List<String> names = new ArrayList<>();
        Matcher matcher = PARAMETER.matcher(text);
        StringBuilder sql = new StringBuilder();
        while (matcher.find()) {
            names.add(matcher.group(1));
            matcher.appendReplacement(sql, "?");
        }
        matcher.appendTail(sql);
        return new NamedSql(sql.toString(), List.copyOf(names), false);
    }

    String sql() {
        return sql;
    }

    PreparedStatement prepare(Connection connection, Parameters parameters) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            if (!genericPlans && statement.isWrapperFor(PGStatement.class)) {
                statement.unwrap(PGStatement.class).setPrepareThreshold(0);
            }
            for (int index = 0; index < names.size(); index++) {
                String name = names.get(index);
                if (!parameters.values.containsKey(name)) {
                    throw new IllegalArgumentException("Missing SQL parameter: " + name);
                }
                Object value = parameters.values.get(name);
                if (value instanceof ArrayValue(String type, @Nullable Object[] elements)) {
                    Array array = connection.createArrayOf(type, elements);
                    statement.setArray(index + 1, array);
                } else {
                    statement.setObject(index + 1, value);
                }
            }
            return statement;
        } catch (SQLException | RuntimeException e) {
            statement.close();
            throw e;
        }
    }

    @SuppressWarnings("ArrayRecordComponent") // only handed to createArrayOf, never compared
    private record ArrayValue(String type, @Nullable Object[] elements) {
    }

    static Parameters parameters() {
        return new Parameters();
    }

    static final class Parameters {

        private final Map<String, @Nullable Object> values = new HashMap<>();

        Parameters set(String name, @Nullable Object value) {
            values.put(name, value);
            return this;
        }

        Parameters array(String name, String type, @Nullable Object[] elements) {
            values.put(name, new ArrayValue(type, elements));
            return this;
        }
    }
}
