package org.example.task2.slice;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** A stand-in for a remote register that is queried once per batch. */
public final class PartsRegister {

    public record Party(String person, int bracket) {
    }

    private final Map<String, Party> parties = new ConcurrentHashMap<>();
    public final AtomicInteger lookups = new AtomicInteger();

    public PartsRegister register(String person, int bracket) {
        parties.put(person, new Party(person, bracket));
        return this;
    }

    public Map<String, Party> lookupAll(Set<String> persons) {
        lookups.incrementAndGet();
        return persons.stream().filter(parties::containsKey).collect(Collectors.toMap(person -> person, parties::get));
    }
}
