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

import org.postgresql.PGStatement;

import org.jspecify.annotations.Nullable;

/** SQL with {@code :name} parameters, so one value can be bound in several places. */
final class NamedSql {

    private static final Pattern PARAMETER = Pattern.compile("(?<![:\\w]):([a-z_]+)");

    private final String sql;
    private final List<String> names;
    private final boolean customPlans;

    private NamedSql(String sql, List<String> names, boolean customPlans) {
        this.sql = sql;
        this.names = names;
        this.customPlans = customPlans;
    }

    /**
     * The same statement, planned for its actual parameters on every execution. For claims: a
     * cached generic plan, made while a topic sat idle and empty, keeps its empty-table shape
     * when work arrives, and was 30 times slower in the Phase 2 benchmark.
     */
    NamedSql withCustomPlans() {
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
            if (customPlans && statement.isWrapperFor(PGStatement.class)) {
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
