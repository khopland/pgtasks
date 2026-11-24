package com.example.orders;

import java.net.URI;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decisions;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Task;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.engine.LocalTransactions;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.jackson.JacksonPayloadCodecs;
import io.github.khopland.pgtasks.junit.TaskEngineExtension;

import com.example.orders.Topics.Notice;
import com.example.orders.Topics.OrderRef;
import com.example.orders.Topics.ShipRequest;
import com.example.orders.Topics.Step;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import tools.jackson.databind.json.JsonMapper;

import static com.example.orders.Topics.NOTIFY;
import static com.example.orders.Topics.PAYMENT;
import static com.example.orders.Topics.SHIP;
import static com.example.orders.Topics.STOCK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The order flow on the in-memory store (docs/testing.md): the real configurers, with fake
 * partners, and the staged SQL recorded instead of run, since the memory store has no connection.
 */
class OrderFlowTest {

    static final FakePartners PARTNERS = new FakePartners();
    static final RecordingWrites WRITES = new RecordingWrites();

    @RegisterExtension
    static final TaskEngineExtension ENGINE = TaskEngineExtension.with(builder -> builder
        .codecs(new JacksonPayloadCodecs(JsonMapper.builder().build()))
        .heartbeat(Duration.ofSeconds(5), Duration.ofMinutes(2))   // as application.yml
        .configure("payment", new PaymentTasks().payments(PARTNERS, WRITES))
        .configure("stock", new StockTasks().stock(WRITES))
        .configure("shipping", new ShippingTasks().shipping(PARTNERS, WRITES, WRITES))
        .configure("notification", new NotificationTasks().notifications(PARTNERS, WRITES)));

    @BeforeEach
    void reset() {
        PARTNERS.reset();
        WRITES.reset();
    }

    private static void place(Tasks tasks, LocalTransactions transactions, String orderId) throws Exception {
        place(tasks, transactions, orderId, true);
    }

    /** With {@code stock} false, the stock step is left out, to be pushed later with {@link #reserveStock}. */
    private static void place(Tasks tasks, LocalTransactions transactions, String orderId, boolean stock) throws Exception {
        OrderRef order = new OrderRef(orderId, "c-1", 2500);
        transactions.inTransaction(() -> {
            tasks.push(PAYMENT, Insertion.APPEND, Creation.of(orderId, order).withDedupKey("pay:" + orderId));
            if (stock) {
                tasks.push(STOCK, orderId, order);
            }
            tasks.push(SHIP, Insertion.APPEND, Creation.of(orderId, new ShipRequest(orderId, "c-1", Step.SHIP)));
            return null;
        });
    }

    private static void reserveStock(Tasks tasks, String orderId) {
        tasks.independently().push(STOCK, orderId, new OrderRef(orderId, "c-1", 2500));
    }

    @Test
    void ships_once_payment_and_stock_are_done_and_tells_the_customer(Tasks tasks, TaskTester tester,
                                                                       LocalTransactions transactions) throws Exception {
        // drain(topic) waits for one topic, but every topic runs meanwhile: hold stock back to see the fan-in wait.
        place(tasks, transactions, "o-1", false);

        tester.drain(PAYMENT);
        tester.awaitIdle();
        assertThat(PARTNERS.calls("/shipments")).as("stock isn't reserved yet").isZero();
        reserveStock(tasks, "o-1");
        tester.drain(STOCK);
        tester.drain(SHIP);
        tester.drain(NOTIFY);

        assertThat(PARTNERS.calls("/payments")).isEqualTo(1);
        assertThat(PARTNERS.calls("/shipments")).isEqualTo(1);
        assertThat(tester.pushedPayloads(NOTIFY)).extracting(Notice::kind).containsExactlyInAnyOrder("shipped", "feedback");
        assertThat(WRITES.statements("o-1")).anyMatch(sql -> sql.contains("'SHIPPED'")).anyMatch(sql -> sql.contains("'COMPLETED'"));
    }

    @Test
    void a_declined_payment_cancels_the_order_and_releases_its_stock(Tasks tasks, TaskTester tester,
                                                                     LocalTransactions transactions) throws Exception {
        PARTNERS.decline.add("o-2");
        place(tasks, transactions, "o-2", false);

        tester.drain(PAYMENT);
        tester.advance(Duration.ofSeconds(20));   // the cancel keeps waiting (later) for the stock step
        tester.awaitIdle();
        assertThat(WRITES.statements("o-2")).noneMatch(sql -> sql.contains("'CANCELLED'"));
        reserveStock(tasks, "o-2");
        tester.drain(STOCK);
        tester.drain(SHIP);
        tester.drain(NOTIFY);

        assertThat(PARTNERS.calls("/shipments")).isZero();
        assertThat(tester.counts(SHIP).suspended()).as("the replaced ship task").isZero();
        assertThat(WRITES.statements("o-2")).anyMatch(sql -> sql.contains("'release'")).anyMatch(sql -> sql.contains("'CANCELLED'"));
        assertThat(tester.pushedPayloads(NOTIFY)).extracting(Notice::kind).containsExactly("declined");
    }

    @Test
    void an_outage_is_retried_without_using_up_attempts(Tasks tasks, TaskTester tester, LocalTransactions transactions) throws Exception {
        PARTNERS.failNext.set(5);
        place(tasks, transactions, "o-3");

        tester.drain(PAYMENT);

        assertThat(PARTNERS.calls("/payments")).isEqualTo(6);
        assertThat(tester.counts(PAYMENT).failed()).isZero();
    }

    /** The partners, answering from memory. */
    static final class FakePartners extends Partners {
        final Set<String> decline = ConcurrentHashMap.newKeySet();
        final AtomicInteger failNext = new AtomicInteger();
        final List<String> calls = new CopyOnWriteArrayList<>();

        FakePartners() {
            super(URI.create("http://unused"), Duration.ofSeconds(1));
        }

        void reset() {
            decline.clear();
            failNext.set(0);
            calls.clear();
        }

        long calls(String path) {
            return calls.stream().filter(path::equals).count();
        }

        private void call(String path) throws Unavailable {
            calls.add(path);
            if (failNext.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new Unavailable(path + " is down", null);
            }
        }

        @Override
        public String pay(String orderId, long amountCents, String idempotencyKey) throws Unavailable, Declined {
            call("/payments");
            if (decline.contains(orderId)) {
                throw new Declined(orderId);
            }
            return "pay-" + orderId;
        }

        @Override
        public String ship(String orderId, String idempotencyKey) throws Unavailable {
            call("/shipments");
            return "TRK-" + orderId;
        }

        @Override
        public String notify(String orderId, String customerId, String kind, String idempotencyKey) throws Unavailable {
            call("/notifications");
            return "sent";
        }
    }

    /** Records the SQL of the tasks that continue, by order id (the statements' last parameter). */
    static final class RecordingWrites extends StagedWrites implements ShippingTasks.OrderStates {
        final Map<String, List<String>> byOrder = new ConcurrentHashMap<>();

        void reset() {
            byOrder.clear();
        }

        List<String> statements(String orderId) {
            return byOrder.getOrDefault(orderId, List.of());
        }

        @Override
        public boolean reserved(String orderId) {
            return statements(orderId).stream().anyMatch(sql -> sql.contains("'reserve'"));
        }

        @Override
        public boolean paid(String orderId) {
            return statements(orderId).stream().anyMatch(sql -> sql.contains("'PAID'"));
        }

        @Override
        public boolean declined(String orderId) {
            return statements(orderId).stream().anyMatch(sql -> sql.contains("'DECLINED'"));
        }

        @Override
        public void beforeCommit(Staged staged, Decisions finalDecisions, Connection connection) {
            for (Task<?> task : finalDecisions.continuing()) {
                staged.statements(task).forEach(statement -> byOrder
                    .computeIfAbsent(String.valueOf(statement.parameters()[statement.parameters().length - 1]), ignored -> new CopyOnWriteArrayList<>())
                    .add(statement.sql()));
            }
        }
    }
}
