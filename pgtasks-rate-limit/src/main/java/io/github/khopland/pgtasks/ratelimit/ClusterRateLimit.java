package io.github.khopland.pgtasks.ratelimit;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.SettingKey;

import org.jspecify.annotations.Nullable;

/**
 * A sliding-window rate limit shared by every node on the store. A topic with a positive
 * {@link #PER_WINDOW} may claim that many tasks per {@link #WINDOW} across the cluster, plus up to
 * {@link #BURST} more that can be spent quickly. As a window advances, the previous one counts for
 * less, so a busy topic takes one claim at a time instead of waiting for the window to turn over.
 * Register one with {@code TaskEngine.builder().extension(new ClusterRateLimit())}; the store needs
 * a {@link SlidingWindowStore} capability, from the module that provides it for that store.
 */
public final class ClusterRateLimit implements Extension {

    /** How long the window is; one minute by default. */
    public static final SettingKey<Duration> WINDOW =
        SettingKey.of("cluster.window", Duration.class, Duration.ofMinutes(1), Duration::parse);

    /** Claims allowed per window across all nodes; zero, the default, leaves the topic unlimited. */
    public static final SettingKey<Double> PER_WINDOW =
        SettingKey.of("cluster.per-window", Double.class, 0.0, ClusterRateLimit::nonNegative);

    /** Extra claims allowed in a burst; zero, the default, allows no burst. */
    public static final SettingKey<Double> BURST =
        SettingKey.of("cluster.burst", Double.class, 0.0, ClusterRateLimit::nonNegative);

    private final @Nullable SlidingWindowStore store;

    /** Finds the shared counter from the engine's store when the extension is installed. */
    public ClusterRateLimit() {
        store = null;
    }

    /** Uses an explicit counter; useful when several engines in one process share it. */
    public ClusterRateLimit(SlidingWindowStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public List<SettingKey<?>> settings() {
        return List.of(WINDOW, PER_WINDOW, BURST);
    }

    @Override
    public void install(ExtensionContext context) {
        SlidingWindowStore counter = store != null ? store : context.capability(SlidingWindowStore.class);
        InstantSource clock = context.clock();
        // One map per installed gate: one extension instance may serve several engines.
        Map<TopicRef<?>, Long> reservations = new ConcurrentHashMap<>();
        context.claimGate(new ClaimGate() {
            @Override
            public Permit permit(TopicRef<?> topic) {
                double limit = context.topicSetting(topic, PER_WINDOW).orElse(0.0);
                if (limit <= 0) {
                    return Permit.ANY;
                }
                Duration window = context.topicSetting(topic, WINDOW).orElse(WINDOW.defaultValue());
                if (window.isZero() || window.isNegative()) {
                    return Permit.ANY;
                }
                double burst = context.topicSetting(topic, BURST).orElse(0.0);
                long now = clock.instant().toEpochMilli();
                SlidingCounter.Admission admitted = counter.update(topic.name(),
                    state -> state.admit(now, window.toMillis(), limit, burst));
                if (admitted.waits()) {
                    return Permit.waitFor(admitted.delay());
                }
                reservations.put(topic, admitted.reserved());
                return Permit.atMost((int) admitted.reserved());
            }

            @Override
            public void claimed(TopicRef<?> topic, int tasks) {
                Long reserved = reservations.remove(topic);
                if (reserved != null) {
                    counter.update(topic.name(), state -> {
                        state.claimed(tasks, reserved);
                        return true;
                    });
                }
            }
        });
    }

    private static double nonNegative(String text) {
        double value = Double.parseDouble(text.trim());
        if (!Double.isFinite(value) || value < 0) {
            throw new IllegalArgumentException("A rate limit must be finite and non-negative, not " + text);
        }
        return value;
    }
}
