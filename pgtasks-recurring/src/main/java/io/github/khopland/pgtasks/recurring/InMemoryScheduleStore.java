package io.github.khopland.pgtasks.recurring;

import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import io.github.khopland.pgtasks.spi.StoreTransaction;

import org.jspecify.annotations.Nullable;

/**
 * Schedules kept in memory, next to the in-memory store's tasks, for local runs and tests. One per
 * store, shared by every engine on it. The in-memory store runs one transaction at a time, so a
 * schedule can't fire twice, and every change is undone with its transaction, or a savepoint
 * before it ({@link StoreTransaction#onRollback}), as the store's own are: a run whose push fails
 * and is rolled back to its savepoint keeps its place.
 */
final class InMemoryScheduleStore implements ScheduleStore {

    private static final Comparator<Key> BY_KEY = Comparator.comparing(Key::topic).thenComparing(Key::name);

    private final InstantSource clock;
    private final Map<Key, Row> rows = new TreeMap<>(BY_KEY);

    InMemoryScheduleStore(InstantSource clock) {
        this.clock = clock;
    }

    @Override
    public synchronized void declare(StoreTransaction transaction, List<Declaration> schedules) {
        Instant now = clock.instant();
        for (Declaration declared : schedules) {
            Row old = rows.get(declared.key());
            Instant next = declared.nextRunAt();
            @Nullable Instant last = null;
            if (old != null) {
                last = old.lastRunAt();
                if (old.cron().equals(declared.cron()) && old.zone().equals(declared.zone())) {
                    next = old.nextRunAt();
                }
            }
            put(transaction, new Row(declared.key(), declared.cron(), declared.zone(), next, last, now));
        }
    }

    @Override
    public synchronized void seen(StoreTransaction transaction, Collection<Key> keys) {
        Instant now = clock.instant();
        for (Key key : keys) {
            Row row = rows.get(key);
            if (row != null) {
                put(transaction, new Row(key, row.cron(), row.zone(), row.nextRunAt(), row.lastRunAt(), now));
            }
        }
    }

    @Override
    public synchronized List<Row> due(StoreTransaction transaction, Collection<Key> keys, int limit) {
        Instant now = clock.instant();
        Set<Key> wanted = new HashSet<>(keys);
        return rows.values().stream()
            .filter(row -> wanted.contains(row.key()) && !row.nextRunAt().isAfter(now))
            .sorted(Comparator.comparing(Row::nextRunAt))
            .limit(limit)
            .toList();
    }

    @Override
    public synchronized void fired(StoreTransaction transaction, Key key, @Nullable Instant firedAt, Instant nextRunAt) {
        Row row = rows.get(key);
        if (row != null) {
            put(transaction, new Row(key, row.cron(), row.zone(), nextRunAt, firedAt == null ? row.lastRunAt() : firedAt, row.seenAt()));
        }
    }

    @Override
    public synchronized int forget(StoreTransaction transaction, Instant before) {
        List<Key> gone = new ArrayList<>();
        rows.values().stream().filter(row -> row.seenAt().isBefore(before)).forEach(row -> gone.add(row.key()));
        gone.forEach(key -> remove(transaction, key));
        return gone.size();
    }

    /** Stores a row, and puts back what was there if the transaction rolls back. */
    private void put(StoreTransaction transaction, Row row) {
        @Nullable Row before = rows.put(row.key(), row);
        transaction.onRollback(() -> restore(row.key(), before));
    }

    private void remove(StoreTransaction transaction, Key key) {
        @Nullable Row before = rows.remove(key);
        if (before != null) {
            transaction.onRollback(() -> restore(key, before));
        }
    }

    private synchronized void restore(Key key, @Nullable Row before) {
        if (before == null) {
            rows.remove(key);
        } else {
            rows.put(key, before);
        }
    }

    @Override
    public synchronized List<Row> list(StoreTransaction transaction) {
        return List.copyOf(rows.values());
    }

    @Override
    public String toString() {
        return "InMemoryScheduleStore[" + rows.size() + " schedule(s)]";
    }
}
