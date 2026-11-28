package io.github.khopland.pgtasks.pause;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.admin.TopicStatus;
import io.github.khopland.pgtasks.engine.extension.ClaimGate;
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.ExtensionContext;
import io.github.khopland.pgtasks.engine.extension.HousekeepingJob;
import io.github.khopland.pgtasks.engine.extension.OperatorView;

import org.jspecify.annotations.Nullable;

/**
 * Pauses and resumes topics on every node. A paused topic still takes pushes, and its running
 * batches finish, but no node claims from it until it is resumed; its tasks wait, and their delays
 * and retries come due as usual. Register one with {@code TaskEngine.builder().extension(new Pause())}
 * or, with Spring, as a bean, and call {@link #pause} and {@link #resume} on it, or use the
 * starter's endpoint and dashboard, which show the paused topics and pause and resume them
 * through its {@code pause} view. The store needs a {@link PauseStore}: this module has one for
 * the in-memory store, and {@code pgtasks-pause-postgres} one for Postgres.
 *
 * <p>A pause is stored, so it outlasts restarts, and announced on the {@value #CHANNEL} channel in
 * the transaction that stores it. Each node reads the paused topics when it starts, before its
 * topics claim anything, keeps them in memory and holds their claims back with a claim gate, so a
 * claim costs nothing more. A node reloads them on that channel's messages, after it lost its
 * notification connection and on every heartbeat, so a lost message delays a change by at most
 * one heartbeat. On a resume each node wakes the topic at once.
 *
 * <p>Install it on every node that runs a paused topic: a node without it claims as if nothing
 * were paused. A node that only pushes, or only operates, can install it to call {@link #pause}
 * and {@link #resume}. One instance serves every engine it is installed in.
 */
public final class Pause implements Extension {

    /** The channel a pause or a resume is announced on; the topic's name is the payload. */
    public static final String CHANNEL = "pause";

    /** How long a paused topic waits before asking again, unless a resume wakes it first. */
    static final Duration PAUSED_WAIT = Duration.ofSeconds(30);

    /** How long a topic waits while this node doesn't know the paused topics, as after a stop. */
    static final Duration UNKNOWN_WAIT = Duration.ofSeconds(1);

    /** How often a node forgets the pauses of deleted topics; one node at a time does it. */
    static final Duration CLEANUP_INTERVAL = Duration.ofHours(1);

    private static final System.Logger LOGGER = System.getLogger(Pause.class.getName());

    private final @Nullable PauseStore explicit;
    private final List<Installed> installed = new CopyOnWriteArrayList<>();

    /** Finds the paused topics' storage from the engine's store when the extension is installed. */
    public Pause() {
        explicit = null;
    }

    /** Uses explicit storage for the paused topics. */
    public Pause(PauseStore store) {
        explicit = Objects.requireNonNull(store, "store");
    }

    @Override
    public void install(ExtensionContext context) {
        Installed engine = new Installed(context, explicit != null ? explicit : context.capability(PauseStore.class));
        installed.add(engine);
        // Before the topics claim anything; fails the start when the storage can't be used.
        context.onStart(engine::start);
        context.onStop(engine::forget);
        context.claimGate(engine);
        context.job(HousekeepingJob.leader("pause cleanup", CLEANUP_INTERVAL, engine::forgetDeletedTopics));
        context.operatorView(engine.view());
    }

    /**
     * Pauses a topic on every node: from when this returns, this node claims nothing from it, and
     * the other nodes stop as soon as the message reaches them. Batches already running finish.
     *
     * @return false if it was paused already
     * @throws IllegalArgumentException for a topic that doesn't exist
     * @throws IllegalStateException    if the extension isn't installed in an engine
     */
    public boolean pause(String topic) {
        return first().pause(Objects.requireNonNull(topic, "topic"));
    }

    /**
     * Resumes a paused topic on every node: each wakes it as the message reaches it, so its tasks
     * are claimed at once.
     *
     * @return false if it wasn't paused
     * @throws IllegalStateException if the extension isn't installed in an engine
     */
    public boolean resume(String topic) {
        return first().resume(Objects.requireNonNull(topic, "topic"));
    }

    /**
     * Every paused topic, by name, as stored.
     *
     * @throws IllegalStateException if the extension isn't installed in an engine
     */
    public List<PauseStore.Paused> paused() {
        return first().read();
    }

    /** Whether a topic is paused, as stored. */
    public boolean isPaused(String topic) {
        Objects.requireNonNull(topic, "topic");
        return paused().stream().anyMatch(paused -> paused.topic().equals(topic));
    }

    private Installed first() {
        if (installed.isEmpty()) {
            throw new IllegalStateException("Pause isn't installed: register it with TaskEngine.builder().extension(...) first");
        }
        return installed.getFirst();
    }

    /** Tells every engine this is installed in to read the paused topics again, as the message would. */
    private void reloadAll() {
        installed.forEach(engine -> engine.reload(Set.of()));
    }

    /** The extension in one engine: the paused topics this node knows of, and the gate that holds them back. */
    private final class Installed implements ClaimGate {
        private final ExtensionContext context;
        private final PauseStore pauses;
        /** What a pause or a resume is sent on, and what every node's reload hears it on. */
        private final ExtensionContext.Channel channel;
        /** By topic, when it was paused; null before a start reads them, and again after a stop. */
        private volatile @Nullable Map<String, Instant> paused;

        Installed(ExtensionContext context, PauseStore pauses) {
            this.context = context;
            this.pauses = pauses;
            // Registered here, while the extension installs, so the channel can be final.
            this.channel = context.channel(CHANNEL, this::reload);
        }

        @Override
        public Permit permit(TopicRef<?> topic, int wanted) {
            Map<String, Instant> known = paused;
            if (known == null) {
                // Only between a stop and the next start, which reads the pauses before any claim.
                return Permit.waitFor(UNKNOWN_WAIT, "pauses not read yet");
            }
            return known.containsKey(topic.name()) ? Permit.waitFor(PAUSED_WAIT, "paused") : Permit.ANY;
        }

        void start() {
            pauses.prepare();
            reloadOrThrow();
        }

        void forget() {
            paused = null;
        }

        /** Reads the paused topics again, and wakes the topics resumed since. Logs a failure and keeps what it knew. */
        void reload(Set<String> topics) {
            try {
                reloadOrThrow();
            } catch (RuntimeException e) {
                LOGGER.log(System.Logger.Level.WARNING, "Could not reload the paused topics; trying again on the next heartbeat", e);
            }
        }

        private synchronized void reloadOrThrow() {
            Map<String, Instant> before = paused;
            Map<String, Instant> now = new TreeMap<>();
            read().forEach(row -> now.put(row.topic(), row.pausedAt()));
            paused = Map.copyOf(now);
            if (before != null) {
                for (String topic : before.keySet()) {
                    if (!now.containsKey(topic)) {
                        context.wake(topic);
                    }
                }
            }
        }

        boolean pause(String topic) {
            boolean changed = context.transact("Pausing " + topic, transaction -> {
                if (context.store().findTopic(transaction, topic).isEmpty()) {
                    throw new IllegalArgumentException("No topic '" + topic + "'");
                }
                boolean paused = pauses.pause(transaction, topic);
                if (paused) {
                    channel.send(transaction, topic);
                }
                return paused;
            });
            reloadAll();
            if (changed) {
                LOGGER.log(System.Logger.Level.INFO, "Paused topic " + topic + " on every node");
            }
            return changed;
        }

        boolean resume(String topic) {
            boolean changed = context.transact("Resuming " + topic, transaction -> {
                boolean resumed = pauses.resume(transaction, topic);
                if (resumed) {
                    channel.send(transaction, topic);
                }
                return resumed;
            });
            reloadAll();
            if (changed) {
                LOGGER.log(System.Logger.Level.INFO, "Resumed topic " + topic + " on every node");
            }
            return changed;
        }

        List<PauseStore.Paused> read() {
            return context.transact("Reading the paused topics", pauses::list);
        }

        /** Deletes the pauses of topics that no longer exist, so a topic made again under the name isn't paused. */
        void forgetDeletedTopics() {
            Set<String> topics = new HashSet<>();
            context.admin().topics().stream().map(TopicStatus::name).forEach(topics::add);
            List<String> forgotten = context.transact("Forgetting the pauses of deleted topics", transaction -> {
                List<String> gone = pauses.list(transaction).stream().map(PauseStore.Paused::topic).filter(name -> !topics.contains(name)).toList();
                for (String topic : gone) {
                    pauses.resume(transaction, topic);
                }
                return gone;
            });
            if (!forgotten.isEmpty()) {
                LOGGER.log(System.Logger.Level.INFO, "Forgot the pauses of deleted topics " + forgotten);
            }
        }

        /** The paused topics, with Resume on each, and Pause or Resume on every topic. */
        OperatorView view() {
            return new OperatorView() {
                @Override
                public String name() {
                    return "pause";
                }

                @Override
                public String title() {
                    return "Paused topics";
                }

                @Override
                public String note() {
                    return "No node claims from a paused topic until it is resumed; its tasks wait, and pushes still go in.";
                }

                @Override
                public List<Column> columns() {
                    return List.of(Column.of("topic", "Topic", Column.Kind.TOPIC), Column.of("since", "Paused", Column.Kind.TIME));
                }

                @Override
                public List<Row> rows() {
                    Map<String, Instant> known = paused;
                    if (known == null) {
                        return List.of();
                    }
                    return new TreeMap<>(known).entrySet().stream().map(entry -> {
                        Map<String, Object> values = new LinkedHashMap<>();
                        values.put("topic", entry.getKey());
                        values.put("since", entry.getValue());
                        return new Row(entry.getKey(), values, Set.of("resume"));
                    }).toList();
                }

                @Override
                public TopicState topic(String topic) {
                    Map<String, Instant> known = paused;
                    return known != null && known.containsKey(topic) ? TopicState.of("paused", "resume") : TopicState.of(null, "pause");
                }

                @Override
                public List<Action> actions() {
                    return List.of(
                        Action.of("pause", "Pause", "No node claims from the topic until it is resumed. Pushes still go in, and running batches finish.",
                            topic -> Installed.this.pause(topic)),
                        Action.of("resume", "Resume", "Every node claims from the topic again at once.", topic -> Installed.this.resume(topic)));
                }
            };
        }
    }
}
