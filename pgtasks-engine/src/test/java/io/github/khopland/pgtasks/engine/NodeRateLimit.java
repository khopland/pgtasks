package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.SettingKey;

/**
 * A per-node rate limit, written only against the extension hooks: a setting, a claim gate and
 * nothing in the engine that knows about it. Each topic with {@code rate.per-second} above zero
 * claims at most that many tasks a second on this node, with a burst of one second's worth.
 */
final class NodeRateLimit implements Extension {

    static final SettingKey<Double> RATE = SettingKey.of("rate.per-second", Double.class, 0.0, Double::valueOf);

    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    @Override
    public List<SettingKey<?>> settings() {
        return List.of(RATE);
    }

    @Override
    public void install(ExtensionContext context) {
        InstantSource clock = context.clock();
        context.claimGate(new ClaimGate() {
            @Override
            public Permit permit(TopicRef<?> topic) {
                double rate = context.topicSetting(topic, RATE).orElse(0.0);
                return rate <= 0 ? Permit.ANY : bucket(topic, rate, clock).permit();
            }

            @Override
            public void claimed(TopicRef<?> topic, int tasks) {
                Bucket bucket = buckets.get(topic.name());
                if (bucket != null) {
                    bucket.take(tasks);
                }
            }
        });
    }

    private Bucket bucket(TopicRef<?> topic, double rate, InstantSource clock) {
        return buckets.computeIfAbsent(topic.name(), ignored -> new Bucket(rate, clock));
    }

    /** A token bucket holding at most one second's worth. */
    private static final class Bucket {
        private final double rate;
        private final InstantSource clock;
        private double tokens;
        private Instant refilled;

        Bucket(double rate, InstantSource clock) {
            this.rate = rate;
            this.clock = clock;
            this.tokens = rate;
            this.refilled = clock.instant();
        }

        synchronized ClaimGate.Permit permit() {
            refill();
            if (tokens >= 1) {
                return ClaimGate.Permit.atMost((int) tokens);
            }
            return ClaimGate.Permit.waitFor(Duration.ofNanos((long) Math.ceil((1 - tokens) / rate * 1e9)));
        }

        synchronized void take(int tasks) {
            refill();
            tokens = Math.max(0, tokens - tasks);
        }

        private void refill() {
            Instant now = clock.instant();
            tokens = Math.min(rate, tokens + Duration.between(refilled, now).toNanos() / 1e9 * rate);
            refilled = now;
        }
    }
}
