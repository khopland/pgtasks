package com.example.orders;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** The payment gateway, the carrier and the mailer, all behind one flaky HTTP stub. */
@Component
public class Partners {

    /** The partner can't take the call now: down, timed out or answering 5xx. */
    public static final class Unavailable extends Exception {
        public Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** The partner asked us to slow down. */
    public static final class Throttled extends Exception {
        private final Duration retryAfter;

        public Throttled(Duration retryAfter) {
            super("throttled for " + retryAfter);
            this.retryAfter = retryAfter;
        }

        public Duration retryAfter() {
            return retryAfter;
        }
    }

    /** A payment the gateway refused for good. */
    public static final class Declined extends Exception {
        public Declined(String orderId) {
            super("payment declined for " + orderId);
        }
    }

    /** The partner refused the request as it is, such as a bad address: retrying won't help until someone fixes it. */
    public static final class Rejected extends Exception {
        public Rejected(String message) {
            super(message);
        }
    }

    private static final Pattern REF = Pattern.compile("\"ref\"\\s*:\\s*\"([^\"]+)\"");

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    private final URI base;
    private final Duration timeout;

    public Partners(@Value("${orders.partners.url:http://localhost:9090}") URI base,
                    @Value("${orders.partners.timeout:2s}") Duration timeout) {
        this.base = base;
        this.timeout = timeout;
    }

    public String pay(String orderId, long amountCents, String idempotencyKey) throws Unavailable, Throttled, Declined, Rejected {
        return call("/payments", orderId, "\"amountCents\":" + amountCents, idempotencyKey);
    }

    public String ship(String orderId, String idempotencyKey) throws Unavailable, Throttled, Declined, Rejected {
        return call("/shipments", orderId, "\"service\":\"standard\"", idempotencyKey);
    }

    public String notify(String orderId, String customerId, String kind, String idempotencyKey) throws Unavailable, Throttled, Declined, Rejected {
        return call("/notifications", orderId, "\"customerId\":\"" + customerId + "\",\"kind\":\"" + kind + "\"", idempotencyKey);
    }

    private String call(String path, String orderId, String fields, String idempotencyKey) throws Unavailable, Throttled, Declined, Rejected {
        HttpRequest request = HttpRequest.newBuilder(base.resolve(path))
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", idempotencyKey)
            .POST(HttpRequest.BodyPublishers.ofString("{\"orderId\":\"" + orderId + "\"," + fields + "}"))
            .build();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new Unavailable(path + " unreachable: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Unavailable(path + " interrupted", e);
        }
        return switch (response.statusCode()) {
            case 200 -> {
                Matcher matcher = REF.matcher(response.body());
                yield matcher.find() ? matcher.group(1) : "?";
            }
            case 402 -> throw new Declined(orderId);
            case 400, 409, 422 -> throw new Rejected(path + " rejected " + orderId + ": " + response.body());
            case 429 -> throw new Throttled(Duration.ofSeconds(response.headers().firstValueAsLong("Retry-After").orElse(1)));
            default -> throw new Unavailable(path + " answered " + response.statusCode() + ": " + response.body(), null);
        };
    }
}
