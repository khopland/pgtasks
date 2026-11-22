package io.github.khopland.pgtasks.admin;

import java.time.Instant;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * A node registered in the store, as of its last heartbeat. Every start registers a new node, and
 * housekeeping forgets nodes that have been gone for long.
 *
 * @param leases the cluster-wide jobs it currently leads, such as {@code housekeeping}
 */
public record NodeView(int id, String name, @Nullable String version, Instant startedAt, Instant heartbeatAt, List<String> leases) {

    public NodeView {
        leases = List.copyOf(leases);
    }
}
