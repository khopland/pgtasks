package io.github.khopland.pgtasks.engine;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import io.github.khopland.pgtasks.engine.extension.ExtensionContext;

/**
 * An extension's reload on a channel, run soon on a thread of its own: told the payloads of the
 * messages since its last run, or nothing in particular (an empty set) when anything may have
 * changed. Requests that arrive while a run waits are served by that run.
 *
 * <p>Until it is {@linkplain #open opened}, once the extensions' start actions have run, it only
 * keeps what arrives: the engine listens before it starts the extensions, so no message is
 * missed, and a reload never runs alongside a start action that reads the same state.
 */
final class ChannelReload {

    /** More payloads than this between two runs are reported as "anything may have changed". */
    static final int MAX_PAYLOADS = 1_000;

    private final ExtensionContext.Reload reload;
    private final Coalesced runs;
    /** Guarded by this. */
    private Set<String> payloads = new LinkedHashSet<>();
    /** Guarded by this: anything may have changed since the last run. */
    private boolean everything;
    /** Guarded by this: whether runs may start; until then, what arrives is kept for the first run. */
    private boolean open;

    ChannelReload(String channel, ExtensionContext.Reload reload) {
        this.reload = reload;
        this.runs = new Coalesced("channel-" + channel, "channel " + channel, this::run);
    }

    /** A message on the channel. */
    void message(String payload) {
        boolean run;
        synchronized (this) {
            if (!everything) {
                payloads.add(payload);
                if (payloads.size() > MAX_PAYLOADS) {
                    everything = true;
                    payloads = new LinkedHashSet<>();
                }
            }
            run = open;
        }
        if (run) {
            runs.request();
        }
    }

    /** Anything may have changed: a lost connection, or the heartbeat's reload in case a message got lost. */
    void everything() {
        boolean run;
        synchronized (this) {
            everything = true;
            payloads = new LinkedHashSet<>();
            run = open;
        }
        if (run) {
            runs.request();
        }
    }

    /** Lets runs start, the extensions having started: one runs at once if anything arrived meanwhile. */
    void open() {
        boolean pending;
        synchronized (this) {
            open = true;
            pending = everything || !payloads.isEmpty();
        }
        if (pending) {
            runs.request();
        }
    }

    private void run() throws Exception {
        Set<String> seen;
        synchronized (this) {
            if (!everything && payloads.isEmpty()) {
                // A request served by the run before: nothing came in since.
                return;
            }
            seen = everything ? Set.of() : Collections.unmodifiableSet(payloads);
            payloads = new LinkedHashSet<>();
            everything = false;
        }
        reload.reload(seen);
    }

    /** Takes requests again after {@link #stop}, for an engine started again; runs wait for {@link #open} again. */
    void restart() {
        synchronized (this) {
            payloads = new LinkedHashSet<>();
            everything = false;
            open = false;
        }
        runs.restart();
    }

    void stop(Duration patience) throws InterruptedException {
        synchronized (this) {
            open = false;
        }
        runs.stop(patience);
    }
}
