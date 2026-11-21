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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

/**
 * Measures what each of the store's statements reads of tasks.queue and tasks.task. Before a
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
 * <p>Statements outside a transaction aren't audited (a savepoint needs one; the store always
 * opens one), nor are the test's own statements, which don't go through {@link #dataSource()}.
 */
final class PlanAudit {

    /** A scan node on the queue or the task table, or one of their indexes, with its actual rows and loops. */
    private static final Pattern NODE = Pattern.compile(
        "(Seq Scan|Index Scan|Index Only Scan|Bitmap Heap Scan|Bitmap Index Scan|Tid Scan|Tid Range Scan)(?: Backward)?(?: using \\S+)? on (\\w+)\\b.*actual rows=(\\d+) loops=(\\d+)");
    private static final Pattern REMOVED = Pattern.compile("Rows Removed by (?:Filter|Index Recheck): (\\d+)");
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

    private final DataSource target;
    private final Budget allowed;
    private final List<String> exempt;
    private final List<Finding> findings = new CopyOnWriteArrayList<>();
    private final AtomicInteger audited = new AtomicInteger();

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

    /** One node of a text plan. Its buffers include its children's, subplans included. */
    private static final class Node {
        final int depth;
        final Node parent;
        final boolean counted;
        final long rows, loops;
        long removed, buffers, childBuffers;

        Node(int depth, Node parent, boolean counted, long rows, long loops) {
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
                while (!path.isEmpty() && path.peek().depth >= depth) {
                    path.pop();
                }
                Node node = counted
                    ? new Node(depth, path.peek(), true, Long.parseLong(scan.group(3)), Long.parseLong(scan.group(4)))
                    : new Node(depth, path.peek(), false, 0, 0);
                nodes.add(node);
                path.push(node);
                continue;
            }
            Node current = nodes.getLast();
            Matcher removed = REMOVED.matcher(line);
            if (removed.find()) {
                current.removed += Long.parseLong(removed.group(1));
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
                rows += (node.rows + node.removed) * node.loops;
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
                audit(connection, sql, setters, values);
            }
            try {
                return method.invoke(statement, arguments);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    private void audit(Connection connection, String sql, List<Method> setters, List<Object[]> values) throws SQLException {
        String text = sql.strip();
        if (connection.getAutoCommit() || text.startsWith("SELECT set_config") || exempt.stream().anyMatch(text::startsWith)) {
            return;
        }
        Savepoint savepoint = connection.setSavepoint();
        StringBuilder plan = new StringBuilder();
        try (PreparedStatement explain = connection.prepareStatement("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF) " + sql)) {
            for (int index = 0; index < setters.size(); index++) {
                setters.get(index).invoke(explain, values.get(index));
            }
            try (ResultSet resultSet = explain.executeQuery()) {
                while (resultSet.next()) {
                    plan.append(resultSet.getString(1)).append('\n');
                }
            }
        } catch (SQLException | ReflectiveOperationException e) {
            // The statement itself will fail the same way, or a concurrent test raced it; nothing to audit.
            plan.setLength(0);
        } finally {
            connection.rollback(savepoint);
        }
        if (!plan.isEmpty()) {
            audited.incrementAndGet();
            Usage usage = usage(plan.toString());
            if (allowed.exceededBy(usage)) {
                findings.add(new Finding(usage, sql, plan.toString()));
            }
        }
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
