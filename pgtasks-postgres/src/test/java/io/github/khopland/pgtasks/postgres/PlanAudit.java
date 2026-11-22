package io.github.khopland.pgtasks.postgres;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.PGStatement;

/**
 * Measures what each of the store's statements reads of pgtasks.queue and pgtasks.task. Before a
 * statement runs, it runs once more as EXPLAIN (ANALYZE, BUFFERS) in a savepoint that is rolled
 * back, so its effects, locks and notifications are undone and only its plan is kept. Two counts,
 * because each misses something the other sees:
 * <ul>
 *   <li>rows examined: what each scan node on those tables returned plus what its filters removed,
 *       on every loop;</li>
 *   <li>buffers: the pages those scan nodes touched. An index condition on a column that doesn't
 *       lead the index walks the whole index but reports only its matches.</li>
 * </ul>
 * Per-node buffers leave out the catalog lookups of a new connection, which swamp a statement's
 * own reads in pg_stat_statements.
 *
 * <p>A statement the store plans for its actual parameters every time (prepare threshold 0) is
 * explained as it is. Any other statement becomes a server-side prepared statement after a few
 * executions, and then Postgres may cache one generic plan for the connection's lifetime: one made
 * while the tables were small or their statistics empty keeps that shape as they grow (the NOTIFY
 * benchmark: a completion walked the topic's whole history). So such a statement is prepared once
 * per connection, the first time the audit sees it, and explained by EXECUTE under
 * {@code force_generic_plan}: the plan Postgres would cache, made from the tables as they were then.
 *
 * <p>Statements outside a transaction aren't audited (a savepoint needs one; the store always
 * opens one), nor are the test's own statements, which don't go through {@link #dataSource()}.
 */
final class PlanAudit {

    /**
     * A scan node on the queue or the task table, or one of their indexes, with its actual rows and
     * loops. Rows are the average per loop: a whole number up to Postgres 17, two decimals from 18 on.
     */
    private static final Pattern NODE = Pattern.compile(
        "(Seq Scan|Index Scan|Index Only Scan|Bitmap Heap Scan|Bitmap Index Scan|Tid Scan|Tid Range Scan)(?: Backward)?(?: using \\S+)? on (\\w+)\\b"
            + ".*actual rows=(\\d+(?:\\.\\d+)?) loops=(\\d+)");
    /** Any scan, to notice one whose line {@link #NODE} no longer reads, as when 18 changed how rows are shown. */
    private static final Pattern ANY_SCAN = Pattern.compile("Scan(?: Backward)?(?: using \\S+)? on (\\w+)\\b.*\\(actual ");
    private static final Pattern REMOVED = Pattern.compile("Rows Removed by (?:Filter|Index Recheck): (\\d+(?:\\.\\d+)?)");
    private static final Pattern BUFFERS = Pattern.compile("Buffers: shared(?: hit=(\\d+))?(?: read=(\\d+))?");
    private static final Set<String> RELATIONS = Set.of("queue", "task");

    /** What a plan read of the queue and task tables. */
    record Usage(long rows, long buffers) {
    }

    /**
     * The most one statement may read: a number of rows, and a few buffers per row on top of what
     * descending the indexes takes.
     */
    record Budget(long rows, long buffers, long buffersPerRow) {

        boolean exceededBy(Usage usage) {
            return usage.rows() > rows || usage.buffers() > buffers + buffersPerRow * usage.rows();
        }

        @Override
        public String toString() {
            return rows + " rows, or " + buffers + " buffers plus " + buffersPerRow + " per row";
        }
    }

    /** One statement whose plan read more than allowed. */
    record Finding(Usage usage, String sql, String plan) {

        @Override
        public String toString() {
            return usage.rows() + " rows examined, " + usage.buffers() + " buffers, by:\n" + sql.strip() + "\nPlan:\n" + plan;
        }
    }

    private static final int TIMEOUT_SECONDS = 20;

    private final DataSource target;
    private final Budget allowed;
    private final List<String> exempt;
    private final List<Finding> findings = new CopyOnWriteArrayList<>();
    private final AtomicInteger audited = new AtomicInteger();
    private final AtomicInteger generic = new AtomicInteger();
    /** Per physical connection: each statement's prepared name, or null if it couldn't be prepared. */
    private final Map<PGConnection, Map<String, String>> prepared = new WeakHashMap<>();
    private final AtomicInteger names = new AtomicInteger();

    /**
     * @param allowed the most one statement may read of the queue and task tables
     * @param exempt  statements, by their start, that read every unfinished task by design
     */
    PlanAudit(DataSource target, Budget allowed, List<String> exempt) {
        this.target = target;
        this.allowed = allowed;
        this.exempt = List.copyOf(exempt);
    }

    DataSource dataSource() {
        return proxy(DataSource.class, target, (method, arguments, result) ->
            method.getName().equals("getConnection") ? connection((Connection) result) : result);
    }

    List<Finding> findings() {
        return List.copyOf(findings);
    }

    /** How many statements were explained. */
    int audited() {
        return audited.get();
    }

    /** How many of them were explained as a cached generic plan. */
    int generic() {
        return generic.get();
    }

    /** One node of a text plan. Its buffers include its children's, subplans included. */
    private static final class Node {
        final int depth;
        final Node parent;
        final boolean counted;
        final double rows;
        final long loops;
        double removed;
        long buffers, childBuffers;

        Node(int depth, Node parent, boolean counted, double rows, long loops) {
            this.depth = depth;
            this.parent = parent;
            this.counted = counted;
            this.rows = rows;
            this.loops = loops;
        }
    }

    /**
     * What the plan read of the queue and task tables: the rows of their scan nodes, and the
     * buffers those nodes touched themselves, without their children's.
     */
    static Usage usage(String plan) {
        List<Node> nodes = new ArrayList<>();
        ArrayDeque<Node> path = new ArrayDeque<>();
        for (String line : plan.split("\n")) {
            if (!nodes.isEmpty() && !line.isEmpty() && !Character.isWhitespace(line.charAt(0))) {
                // Planning, triggers: the plan itself is over.
                break;
            }
            int arrow = line.indexOf("->");
            if (arrow >= 0 || nodes.isEmpty()) {
                int depth = arrow >= 0 ? arrow : 0;
                Matcher scan = NODE.matcher(line);
                boolean counted = scan.find() && RELATIONS.contains(relation(scan.group(2)));
                Matcher any = ANY_SCAN.matcher(line);
                if (!counted && any.find() && RELATIONS.contains(relation(any.group(1)))) {
                    // Counting nothing here would pass every statement: fail instead.
                    throw new IllegalStateException("A scan of the queue or task table the audit can't read: " + line.strip());
                }
                while (!path.isEmpty() && path.peek().depth >= depth) {
                    path.pop();
                }
                Node node = counted
                    ? new Node(depth, path.peek(), true, Double.parseDouble(scan.group(3)), Long.parseLong(scan.group(4)))
                    : new Node(depth, path.peek(), false, 0, 0);
                nodes.add(node);
                path.push(node);
                continue;
            }
            Node current = nodes.getLast();
            Matcher removed = REMOVED.matcher(line);
            if (removed.find()) {
                current.removed += Double.parseDouble(removed.group(1));
            }
            Matcher read = BUFFERS.matcher(line);
            if (read.find()) {
                current.buffers = (read.group(1) == null ? 0 : Long.parseLong(read.group(1))) + (read.group(2) == null ? 0 : Long.parseLong(read.group(2)));
            }
        }
        for (Node node : nodes) {
            if (node.parent != null) {
                node.parent.childBuffers += node.buffers;
            }
        }
        long rows = 0, buffers = 0;
        for (Node node : nodes) {
            if (node.counted) {
                // Both are averages per loop.
                rows += Math.round((node.rows + node.removed) * node.loops);
                buffers += Math.max(0, node.buffers - node.childBuffers);
            }
        }
        return new Usage(rows, buffers);
    }

    /** queue_ready_idx belongs to queue, task_completed_idx to task. */
    private static String relation(String name) {
        for (String relation : RELATIONS) {
            if (name.equals(relation) || name.startsWith(relation + "_")) {
                return relation;
            }
        }
        return name;
    }

    private Connection connection(Connection connection) {
        return proxy(Connection.class, connection, (method, arguments, result) ->
            method.getName().equals("prepareStatement") && arguments.length == 1
                ? statement(connection, (String) arguments[0], (PreparedStatement) result)
                : result);
    }

    private PreparedStatement statement(Connection connection, String sql, PreparedStatement statement) {
        List<Method> setters = new ArrayList<>();
        List<Object[]> values = new ArrayList<>();
        return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[] {PreparedStatement.class}, (proxy, method, arguments) -> {
            String name = method.getName();
            if (name.startsWith("set") && arguments != null && arguments.length >= 2 && arguments[0] instanceof Integer) {
                setters.add(method);
                values.add(arguments);
            } else if (name.startsWith("execute") && (arguments == null || arguments.length == 0)) {
                audit(connection, sql, setters, values, statement.unwrap(PGStatement.class).getPrepareThreshold() == 0);
            }
            try {
                return method.invoke(statement, arguments);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    private void audit(Connection connection, String sql, List<Method> setters, List<Object[]> values, boolean custom) throws SQLException {
        String text = sql.strip();
        if (connection.getAutoCommit() || text.startsWith("SELECT set_config") || exempt.stream().anyMatch(text::startsWith)) {
            return;
        }
        Savepoint savepoint = connection.setSavepoint();
        String explained = "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF) ";
        if (!custom) {
            String name = prepare(connection, sql, savepoint);
            if (name == null) {
                connection.rollback(savepoint);
                return;
            }
            // As literals: an EXECUTE inside EXPLAIN can't take bind parameters. Each is coerced to the
            // type PREPARE inferred.
            Map<Integer, String> literals = new HashMap<>();
            for (int index = 0; index < setters.size(); index++) {
                Object[] arguments = values.get(index);
                literals.put((Integer) arguments[0], setters.get(index).getName().equals("setNull") ? "NULL" : literal(arguments[1]));
            }
            int parameters = literals.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
            StringBuilder execute = new StringBuilder("EXECUTE ").append(name);
            for (int index = 1; index <= parameters; index++) {
                execute.append(index == 1 ? "(" : ", ").append(literals.getOrDefault(index, "NULL")).append(index == parameters ? ")" : "");
            }
            try (PreparedStatement mode = connection.prepareStatement("SET LOCAL plan_cache_mode = force_generic_plan")) {
                mode.execute();
            }
            explained += execute;
        } else {
            explained += sql;
        }
        // A plan quadratic in the history can run for many minutes; past this it has read too much anyway.
        try (PreparedStatement timeout = connection.prepareStatement("SET LOCAL statement_timeout = '" + TIMEOUT_SECONDS + "s'")) {
            timeout.execute();
        }
        StringBuilder plan = new StringBuilder();
        try (PreparedStatement explain = connection.prepareStatement(explained)) {
            for (int index = 0; custom && index < setters.size(); index++) {
                setters.get(index).invoke(explain, values.get(index));
            }
            try (ResultSet resultSet = explain.executeQuery()) {
                while (resultSet.next()) {
                    plan.append(resultSet.getString(1)).append('\n');
                }
            }
        } catch (SQLException e) {
            if ("57014".equals(e.getSQLState())) {
                // Cancelled by the timeout (the statement's own timeouts are the store's business).
                findings.add(new Finding(new Usage(Long.MAX_VALUE, Long.MAX_VALUE), sql, "(explaining it took over " + TIMEOUT_SECONDS + " s)"));
            }
            // Otherwise the statement itself will fail the same way, or a concurrent test raced it; nothing to audit.
            plan.setLength(0);
        } catch (ReflectiveOperationException e) {
            plan.setLength(0);
        } finally {
            connection.rollback(savepoint);
        }
        if (!plan.isEmpty()) {
            audited.incrementAndGet();
            if (!custom) {
                generic.incrementAndGet();
            }
            Usage usage = usage(plan.toString());
            if (allowed.exceededBy(usage)) {
                findings.add(new Finding(usage, sql, plan.toString()));
            }
        }
    }

    /**
     * The statement's prepared name on this physical connection, prepared on first sight, or null if
     * it can't be prepared without the driver's parameter types. Inside the savepoint, which a failed
     * PREPARE leaves to be rolled back; a prepared statement itself outlives the transaction.
     */
    private @Nullable String prepare(Connection connection, String sql, Savepoint savepoint) throws SQLException {
        PGConnection physical = connection.unwrap(PGConnection.class);
        Map<String, String> statements;
        synchronized (prepared) {
            statements = prepared.computeIfAbsent(physical, key -> new HashMap<>());
        }
        if (statements.containsKey(sql)) {
            return statements.get(sql);
        }
        String name = "pgtasks_audit_" + names.incrementAndGet();
        try (PreparedStatement prepare = connection.prepareStatement("PREPARE " + name + " AS " + numbered(sql))) {
            prepare.execute();
        } catch (SQLException e) {
            connection.rollback(savepoint);
            name = null;
        }
        statements.put(sql, name);
        return name;
    }

    private static String literal(@Nullable Object value) throws SQLException {
        if (value == null) {
            return "NULL";
        }
        String text = value instanceof java.sql.Array array ? array.toString() : value.toString();
        return value instanceof Number || value instanceof Boolean ? text : "'" + text.replace("'", "''") + "'";
    }

    /** JDBC's ? placeholders as $1, $2 and so on, outside quotes. */
    static String numbered(String sql) {
        StringBuilder result = new StringBuilder(sql.length() + 16);
        int parameter = 0;
        char quote = 0;
        for (int index = 0; index < sql.length(); index++) {
            char character = sql.charAt(index);
            if (quote != 0) {
                if (character == quote) {
                    quote = 0;
                }
            } else if (character == '\'' || character == '"') {
                quote = character;
            } else if (character == '?') {
                result.append('$').append(++parameter);
                continue;
            }
            result.append(character);
        }
        return result.toString();
    }

    @FunctionalInterface
    private interface After {
        Object apply(Method method, Object[] arguments, Object result) throws Throwable;
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, T target, After after) {
        InvocationHandler handler = (proxy, method, arguments) -> {
            Object result;
            try {
                result = method.invoke(target, arguments);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            return after.apply(method, arguments == null ? new Object[0] : arguments, result);
        };
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }
}
