package io.github.khopland.pgtasks.ratelimit;

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
 * A per-node rate limit on claims. A topic with {@link #PER_SECOND} above zero claims at most that
 * many tasks a second on this node, with a burst of {@link #BURST} tasks (one second's worth by
 * default). Other nodes have their own buckets, so the limit is per node, not per cluster.
 * Register a fresh instance with {@code TaskEngine.builder().extension(new RateLimit())} or, with
 * Spring, as an {@link Extension} bean.
 */
public final class RateLimit implements Extension {

    /** Tasks a second on this node; zero, the default, leaves the topic unlimited. Finite and not negative. */
    public static final SettingKey<Double> PER_SECOND =
        SettingKey.of("rate.per-second", Double.class, 0.0, Double::valueOf).checking(RateLimit::nonNegative);

    /** The burst in tasks; zero, the default, uses one second's worth of {@link #PER_SECOND}. Finite and not negative. */
    public static final SettingKey<Double> BURST =
        SettingKey.of("rate.burst", Double.class, 0.0, Double::valueOf).checking(RateLimit::nonNegative);

    /** Creates the extension; one instance serves every topic of the engines it is installed in, each with buckets of its own. */
    public RateLimit() {
    }

    @Override
    public List<SettingKey<?>> settings() {
        return List.of(PER_SECOND, BURST);
    }

    @Override
    public void install(ExtensionContext context) {
        InstantSource clock = context.clock();
        Buckets buckets = new Buckets();
        context.claimGate(new ClaimGate() {
            @Override
            public Permit permit(TopicRef<?> topic, int wanted) {
                double rate = context.topicSetting(topic, PER_SECOND).orElse(0.0);
                if (rate <= 0) {
                    return Permit.ANY;
                }
                double burst = context.topicSetting(topic, BURST).orElse(0.0);
                return buckets.permit(topic.name(), rate, burst, clock);
            }

            @Override
            public void claimed(TopicRef<?> topic, List<Long> taskIds) {
                if (!taskIds.isEmpty()) {
                    buckets.taken(topic.name(), taskIds.size());
                }
            }
        });
    }

    private static void nonNegative(double value) {
        if (!Double.isFinite(value) || value < 0) {
            throw new IllegalArgumentException("a rate or burst must be finite and not negative, not " + value);
        }
    }

    /** One token bucket per topic name, keyed the way the gate is asked. */
    static final class Buckets {
        private final Map<String, Bucket> byTopic = new ConcurrentHashMap<>();

        ClaimGate.Permit permit(String topic, double rate, double burst, InstantSource clock) {
            Bucket bucket = byTopic.computeIfAbsent(topic, ignored -> new Bucket(clock));
            return bucket.permit(rate, burst);
        }

        void taken(String topic, int tasks) {
            Bucket bucket = byTopic.get(topic);
            if (bucket != null) {
                bucket.take(tasks);
            }
        }
    }

    /** A bucket holding at most the topic's burst, refilled at its rate. */
    private static final class Bucket {
        private final InstantSource clock;
        private double tokens;
        private double capacity = -1;
        private Instant refilled;

        Bucket(InstantSource clock) {
            this.clock = clock;
            this.refilled = clock.instant();
        }

        synchronized ClaimGate.Permit permit(double rate, double burst) {
            double wanted = Math.max(1, burst > 0 ? burst : rate);
            if (wanted != capacity) {
                // A new bucket starts full; a changed burst only ever reduces what is left.
                tokens = capacity < 0 ? wanted : Math.min(tokens, wanted);
                capacity = wanted;
            }
            refill(rate);
            if (tokens >= 1) {
                return ClaimGate.Permit.atMost((int) Math.min(tokens, Integer.MAX_VALUE));
            }
            long nanos = (long) Math.ceil((1 - tokens) / rate * 1e9);
            return ClaimGate.Permit.waitFor(Duration.ofNanos(Math.max(1, nanos)));
        }

        synchronized void take(int tasks) {
            tokens = Math.max(0, tokens - tasks);
        }

        private void refill(double rate) {
            Instant now = clock.instant();
            if (now.isAfter(refilled)) {
                tokens = Math.min(capacity, tokens + Duration.between(refilled, now).toNanos() / 1e9 * rate);
            }
            refilled = now;
        }
    }
}
