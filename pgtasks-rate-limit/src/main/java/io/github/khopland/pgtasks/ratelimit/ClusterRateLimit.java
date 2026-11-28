package io.github.khopland.pgtasks.ratelimit;

import java.time.Duration;
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
 * {@link #PER_WINDOW} may claim that many tasks per {@link #WINDOW} across the cluster. An idle
 * topic can spend a whole window's allowance at once; a busy one gets claims back one at a time as
 * the previous window counts for less, instead of a fresh allowance at each window boundary.
 * Register one with {@code TaskEngine.builder().extension(new ClusterRateLimit())}; the store needs
 * a {@link SlidingWindowStore} capability, from the module that provides it for that store.
 *
 * <p>The engine doesn't start when the counter can't be readied, such as when its table is missing
 * in schema mode {@code VALIDATE}. A counter that fails later holds the topic back instead of
 * letting it claim without a limit.
 */
public final class ClusterRateLimit implements Extension {

    /** How long the window is; one minute by default, and at least a millisecond. */
    public static final SettingKey<Duration> WINDOW =
        SettingKey.of("cluster.window", Duration.class, Duration.ofMinutes(1), Duration::parse).checking(ClusterRateLimit::window);

    /** Claims allowed per window across all nodes; zero, the default, leaves the topic unlimited. */
    public static final SettingKey<Long> PER_WINDOW =
        SettingKey.of("cluster.per-window", Long.class, 0L, Long::valueOf).checking(ClusterRateLimit::nonNegative);

    /** How long a topic waits after the shared counter failed, before asking it again. */
    static final Duration FAILED_COUNTER_WAIT = Duration.ofSeconds(1);

    private static final System.Logger LOGGER = System.getLogger(ClusterRateLimit.class.getName());

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
        return List.of(WINDOW, PER_WINDOW);
    }

    @Override
    public void install(ExtensionContext context) {
        SlidingWindowStore counter = store != null ? store : context.capability(SlidingWindowStore.class);
        // One map per installed gate: one extension instance may serve several engines. A topic
        // polls on one thread per engine, so it holds at most one reservation at a time.
        Map<TopicRef<?>, SlidingCounter.Admission> reservations = new ConcurrentHashMap<>();
        // Fails the start, not the first claim, when the counter can't be used.
        context.onStart(counter::prepare);
        context.claimGate(new ClaimGate() {
            @Override
            public Permit permit(TopicRef<?> topic, int wanted) {
                long limit = context.topicSetting(topic, PER_WINDOW).orElse(0L);
                if (limit <= 0) {
                    return Permit.ANY;
                }
                long window = context.topicSetting(topic, WINDOW).orElse(WINDOW.defaultValue()).toMillis();
                // A reservation left from a permit the engine never settled is handed back first.
                SlidingCounter.Admission stale = reservations.remove(topic);
                SlidingCounter.Admission admitted;
                try {
                    admitted = counter.update(topic.name(), (state, now) -> {
                        if (stale != null) {
                            state.settle(stale, 0, now);
                        }
                        return state.admit(now, window, limit, Math.max(1, wanted));
                    });
                } catch (RuntimeException e) {
                    // The engine claims without a gate that throws; a limit that can't be counted
                    // holds the topic back instead. A stale reservation fades with its window.
                    LOGGER.log(System.Logger.Level.WARNING,
                        "The shared rate limit counter failed for " + topic + "; holding claims back", e);
                    return Permit.waitFor(FAILED_COUNTER_WAIT);
                }
                if (admitted.waits()) {
                    return Permit.waitFor(admitted.delay());
                }
                reservations.put(topic, admitted);
                return Permit.atMost((int) admitted.reserved());
            }

            @Override
            public void claimed(TopicRef<?> topic, List<Long> taskIds) {
                SlidingCounter.Admission admitted = reservations.remove(topic);
                int tasks = taskIds.size();
                if (admitted != null && tasks < admitted.reserved()) {
                    // Only a claim that got fewer than it reserved has anything to give back.
                    counter.update(topic.name(), (state, now) -> {
                        state.settle(admitted, tasks, now);
                        return true;
                    });
                }
            }
        });
    }

    private static void window(Duration window) {
        if (window.toMillis() < 1) {
            throw new IllegalArgumentException("a rate limit window must be at least a millisecond, not " + window);
        }
    }

    private static void nonNegative(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("a rate limit must not be negative, not " + value);
        }
    }
}
