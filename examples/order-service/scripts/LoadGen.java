import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Places orders at a steady rate with bursts, spread over the app instances like a load balancer
 * would (the next instance on a refused connection), and resends some requests with the same
 * request id, as a client retrying after a timeout would.
 *
 * <pre>
 * java scripts/LoadGen.java --rate 40 --minutes 30 --burst-every 120 --burst 600 --urls http://localhost:8081,http://localhost:8082
 * </pre>
 */
public class LoadGen {

    public static void main(String[] args) throws Exception {
        Map<String, String> options = new TreeMap<>(Map.of("rate", "40", "minutes", "10", "burst-every", "120", "burst", "600",
            "urls", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "customers", "3000", "resend", "0.02", "prefix", "r"));
        for (int i = 0; i + 1 < args.length; i += 2) {
            options.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        double rate = Double.parseDouble(options.get("rate"));
        long minutes = Long.parseLong(options.get("minutes"));
        int burstEvery = Integer.parseInt(options.get("burst-every"));
        int burst = Integer.parseInt(options.get("burst"));
        int customers = Integer.parseInt(options.get("customers"));
        double resend = Double.parseDouble(options.get("resend"));
        String prefix = options.get("prefix") + "-" + System.currentTimeMillis() + "-";
        List<URI> urls = List.of(options.get("urls").split(",")).stream().map(url -> URI.create(url + "/orders")).toList();
        System.out.println("load: " + options);

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
        String auth = "Basic " + Base64.getEncoder().encodeToString("api:api".getBytes());
        Map<String, AtomicLong> results = new ConcurrentHashMap<>();
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicLong sequence = new AtomicLong();
        AtomicLong next = new AtomicLong();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            long start = System.nanoTime();
            long end = start + Duration.ofMinutes(minutes).toNanos();
            long lastReport = start;
            long sentAtReport = 0;
            double debt = 0;
            long lastTick = start;
            while (System.nanoTime() < end) {
                long now = System.nanoTime();
                long second = (now - start) / 1_000_000_000L;
                boolean bursting = burstEvery > 0 && second % burstEvery >= burstEvery - 2 && second > 2;
                double currentRate = rate + (bursting ? burst / 2.0 : 0);
                debt += currentRate * (now - lastTick) / 1e9;
                lastTick = now;
                while (debt >= 1) {
                    debt--;
                    long n = sequence.incrementAndGet();
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    String requestId = random.nextDouble() < resend && n > 10 ? prefix + random.nextLong(1, n) : prefix + n;
                    String customer = "c-" + random.nextInt(customers);
                    StringBuilder lines = new StringBuilder();
                    int count = random.nextInt(1, 4);
                    int firstSku = random.nextInt(1, 49);
                    for (int line = 0; line < count; line++) {
                        lines.append(line == 0 ? "" : ",").append("{\"sku\":\"sku-").append(firstSku + line)
                            .append("\",\"quantity\":").append(random.nextInt(1, 5)).append('}');
                    }
                    String body = "{\"requestId\":\"" + requestId + "\",\"customerId\":\"" + customer + "\",\"lines\":[" + lines + "]}";
                    executor.submit(() -> send(client, urls, next, auth, requestId, body, results, latencies));
                }
                if (now - lastReport >= 10_000_000_000L) {
                    report(results, latencies, sequence.get() - sentAtReport, (now - lastReport) / 1e9);
                    sentAtReport = sequence.get();
                    lastReport = now;
                }
                Thread.sleep(5);
            }
        }
        System.out.println("done: sent " + sequence.get() + " " + new TreeMap<>(results));
    }

    static void send(HttpClient client, List<URI> urls, AtomicLong next, String auth, String requestId, String body,
                     Map<String, AtomicLong> results, List<Long> latencies) {
        long started = System.nanoTime();
        for (int tries = 0; tries < urls.size() * 3; tries++) {
            URI url = urls.get((int) (next.getAndIncrement() % urls.size()));
            try {
                HttpResponse<String> response = client.send(HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(10))
                    .header("Authorization", auth).header("Content-Type", "application/json").header("X-Request-Id", requestId)
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() >= 500 || response.statusCode() == 503) {
                    count(results, "http-" + response.statusCode() + "-retried");
                    Thread.sleep(200);
                    continue;
                }
                count(results, "http-" + response.statusCode());
                latencies.add((System.nanoTime() - started) / 1_000_000L);
                return;
            } catch (ConnectException e) {
                count(results, "refused-retried");
            } catch (Exception e) {
                count(results, e.getClass().getSimpleName() + "-retried");
                sleep(200);
            }
        }
        count(results, "gave-up");
    }

    static void report(Map<String, AtomicLong> results, List<Long> latencies, long sent, double seconds) {
        List<Long> copy;
        synchronized (latencies) {
            copy = new ArrayList<>(latencies);
            latencies.clear();
        }
        Collections.sort(copy);
        String percentiles = copy.isEmpty() ? "-" : "p50=" + copy.get(copy.size() / 2) + "ms p99=" + copy.get((int) (copy.size() * 0.99))
            + "ms max=" + copy.getLast() + "ms";
        System.out.printf("%s %.1f/s %s %s%n", LocalTime.now().withNano(0), sent / seconds, percentiles, new TreeMap<>(results));
    }

    static void count(Map<String, AtomicLong> results, String key) {
        results.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
