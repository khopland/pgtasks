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

    ChannelReload(String channel, ExtensionContext.Reload reload) {
        this.reload = reload;
        this.runs = new Coalesced("channel-" + channel, "channel " + channel, this::run);
    }

    /** A message on the channel. */
    void message(String payload) {
        synchronized (this) {
            if (!everything) {
                payloads.add(payload);
                if (payloads.size() > MAX_PAYLOADS) {
                    everything = true;
                    payloads = new LinkedHashSet<>();
                }
            }
        }
        runs.request();
    }

    /** Anything may have changed: a lost connection, or the heartbeat's reload in case a message got lost. */
    void everything() {
        synchronized (this) {
            everything = true;
            payloads = new LinkedHashSet<>();
        }
        runs.request();
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

    /** Takes requests again after {@link #stop}, for an engine started again. */
    void restart() {
        synchronized (this) {
            payloads = new LinkedHashSet<>();
            everything = false;
        }
        runs.restart();
    }

    void stop(Duration patience) throws InterruptedException {
        runs.stop(patience);
    }
}
