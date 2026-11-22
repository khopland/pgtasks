import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * The order service's external partners in one process: a payment gateway, a carrier and a mailer.
 * Slow, flaky and sometimes down, under our control through an admin port.
 *
 * <pre>
 * java stub/Stub.java [port] [adminPort]                         # 9090, 9091
 * curl -XPOST 'localhost:9091/config?fail=1.0'                   # every call fails with 500
 * curl -XPOST 'localhost:9091/config?down=true'                  # connection refused
 * curl -XPOST 'localhost:9091/config?fail=0.02&amp;throttle=0.01&amp;lost=0.005&amp;latency=40&amp;slow=0.01'
 * curl localhost:9091/stats ; curl localhost:9091/dump > calls.csv
 * </pre>
 *
 * Every call carries an Idempotency-Key. The first successful call per key performs the effect;
 * later calls with the key replay its result. "lost" performs the effect but answers 500, as if the
 * reply were lost. The dump lists per endpoint, order and key how often it was called and how often
 * it succeeded, so the caller can count real side effects: distinct keys with a success per order.
 */
public class Stub {

    record Key(String endpoint, String orderId, String key) {
    }

    static final class Calls {
        final AtomicLong calls = new AtomicLong();
        final AtomicLong ok = new AtomicLong();
        volatile String result;
    }

    static final Map<Key, Calls> CALLS = new ConcurrentHashMap<>();
    static final Map<String, AtomicLong> STATUS = new ConcurrentHashMap<>();
    static final Pattern ORDER = Pattern.compile("\"orderId\"\\s*:\\s*\"([^\"]+)\"");
    static final AtomicLong SEQUENCE = new AtomicLong();

    static volatile double fail = 0.02;      // 500 without effect
    static volatile double lost = 0.005;     // effect done, but 500
    static volatile double throttle = 0.01;  // 429 with Retry-After
    static volatile double slow = 0.01;      // a 3 s answer
    static volatile int latency = 40;        // mean latency in ms
    static volatile int decline = 3;         // per cent of orders a payment declines, by order id

    static volatile HttpServer server;
    static int port;

    public static void main(String[] args) throws IOException {
        port = args.length > 0 ? Integer.parseInt(args[0]) : 9090;
        int adminPort = args.length > 1 ? Integer.parseInt(args[1]) : 9091;
        start();
        HttpServer admin = HttpServer.create(new InetSocketAddress(adminPort), 50);
        admin.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        admin.createContext("/config", Stub::config);
        admin.createContext("/stats", exchange -> send(exchange, 200, stats()));
        admin.createContext("/dump", exchange -> send(exchange, 200, dump()));
        admin.createContext("/reset", exchange -> {
            CALLS.clear();
            STATUS.clear();
            send(exchange, 200, "reset\n");
        });
        admin.start();
        System.out.println("stub on " + port + ", admin on " + adminPort + "; " + settings());
    }

    static synchronized void start() throws IOException {
        if (server != null) {
            return;
        }
        HttpServer created = HttpServer.create(new InetSocketAddress(port), 500);
        created.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        created.createContext("/payments", exchange -> effect(exchange, "payments"));
        created.createContext("/shipments", exchange -> effect(exchange, "shipments"));
        created.createContext("/notifications", exchange -> effect(exchange, "notifications"));
        created.start();
        server = created;
    }

    static synchronized void stop() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    static void effect(HttpExchange exchange, String endpoint) throws IOException {
        String body = read(exchange.getRequestBody());
        String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
        Matcher matcher = ORDER.matcher(body);
        if (key == null || !matcher.find()) {
            send(exchange, 400, "{\"error\":\"Idempotency-Key and orderId required\"}");
            return;
        }
        String orderId = matcher.group(1);
        Calls calls = CALLS.computeIfAbsent(new Key(endpoint, orderId, key), ignored -> new Calls());
        calls.calls.incrementAndGet();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        sleep(random.nextDouble() < slow ? 3000 : (long) (latency * (0.5 + random.nextDouble())));
        double roll = random.nextDouble();
        if (roll < throttle) {
            exchange.getResponseHeaders().add("Retry-After", "2");
            count(endpoint, 429);
            send(exchange, 429, "{\"error\":\"slow down\"}");
            return;
        }
        if (roll < throttle + fail) {
            count(endpoint, 500);
            send(exchange, 500, "{\"error\":\"internal error\"}");
            return;
        }
        if (endpoint.equals("payments") && Math.floorMod(orderId.hashCode(), 100) < decline) {
            count(endpoint, 402);
            send(exchange, 402, "{\"error\":\"declined\"}");
            return;
        }
        String result;
        synchronized (calls) {
            if (calls.result == null) {
                calls.result = switch (endpoint) {
                    case "payments" -> "pay-" + SEQUENCE.incrementAndGet();
                    case "shipments" -> "TRK" + SEQUENCE.incrementAndGet();
                    default -> "sent-" + SEQUENCE.incrementAndGet();
                };
            }
            result = calls.result;
            calls.ok.incrementAndGet();
        }
        if (random.nextDouble() < lost) {
            count(endpoint, 599);
            send(exchange, 500, "{\"error\":\"reply lost\"}");
            return;
        }
        count(endpoint, 200);
        send(exchange, 200, "{\"ref\":\"" + result + "\"}");
    }

    static void config(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        if (query != null) {
            for (String pair : query.split("&")) {
                String[] parts = pair.split("=", 2);
                String value = parts.length > 1 ? parts[1] : "";
                switch (parts[0]) {
                    case "fail" -> fail = Double.parseDouble(value);
                    case "lost" -> lost = Double.parseDouble(value);
                    case "throttle" -> throttle = Double.parseDouble(value);
                    case "slow" -> slow = Double.parseDouble(value);
                    case "latency" -> latency = Integer.parseInt(value);
                    case "decline" -> decline = Integer.parseInt(value);
                    case "down" -> {
                        if (Boolean.parseBoolean(value)) {
                            stop();
                        } else {
                            start();
                        }
                    }
                    default -> {
                        send(exchange, 400, "unknown setting " + parts[0] + "\n");
                        return;
                    }
                }
            }
        }
        System.out.println("config: " + settings());
        send(exchange, 200, settings() + "\n");
    }

    static String settings() {
        return "fail=" + fail + " lost=" + lost + " throttle=" + throttle + " slow=" + slow + " latency=" + latency + "ms decline=" + decline
            + "% down=" + (server == null);
    }

    static String stats() {
        Map<String, long[]> perEndpoint = new TreeMap<>();
        Map<String, Map<String, Integer>> effects = new TreeMap<>();
        CALLS.forEach((key, calls) -> {
            long[] totals = perEndpoint.computeIfAbsent(key.endpoint(), ignored -> new long[3]);
            totals[0] += calls.calls.get();
            totals[1]++;
            if (calls.ok.get() > 0) {
                effects.computeIfAbsent(key.endpoint(), ignored -> new ConcurrentHashMap<>()).merge(key.orderId(), 1, Integer::sum);
            }
        });
        StringBuilder out = new StringBuilder(settings()).append('\n');
        perEndpoint.forEach((endpoint, totals) -> {
            Map<String, Integer> byOrder = effects.getOrDefault(endpoint, Map.of());
            long duplicated = byOrder.values().stream().filter(count -> count > 1).count();
            out.append(endpoint).append(": calls=").append(totals[0]).append(" keys=").append(totals[1])
                .append(" orders with effect=").append(byOrder.size()).append(" orders with >1 effect=").append(duplicated).append('\n');
        });
        out.append("status: ").append(new TreeMap<>(STATUS)).append('\n');
        return out.toString();
    }

    static String dump() {
        StringBuilder out = new StringBuilder("endpoint,order_id,idempotency_key,calls,ok\n");
        CALLS.forEach((key, calls) -> out.append(key.endpoint()).append(',').append(key.orderId()).append(',').append(key.key()).append(',')
            .append(calls.calls.get()).append(',').append(calls.ok.get()).append('\n'));
        return out.toString();
    }

    static void count(String endpoint, int status) {
        STATUS.computeIfAbsent(endpoint + ":" + status, ignored -> new AtomicLong()).incrementAndGet();
    }

    static String read(InputStream in) throws IOException {
        try (in) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static void send(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", body.startsWith("{") ? "application/json" : "text/plain");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
