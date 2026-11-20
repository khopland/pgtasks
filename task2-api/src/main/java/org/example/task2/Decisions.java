package org.example.task2;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The decisions for one batch: one {@link Decision} per task, plus the downstream work each task
 * staged. Immutable; {@link #revise} and {@link #merge} return new instances.
 */
public final class Decisions {

    /** One task's decision and the downstream work and junctions it staged. */
    public record Verdict(Task<?> task, Decision decision, List<Push<?>> downstream, List<Junction> junctions) {

        public Verdict {
            Objects.requireNonNull(task, "task");
            Objects.requireNonNull(decision, "decision");
            downstream = List.copyOf(downstream);
            junctions = List.copyOf(junctions);
        }

        public Verdict(Task<?> task, Decision decision, List<Push<?>> downstream) {
            this(task, decision, downstream, List.of());
        }
    }

    private final Map<Long, Verdict> verdicts;

    private Decisions(Map<Long, Verdict> verdicts) {
        this.verdicts = Collections.unmodifiableMap(verdicts);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The same decision for every task, without downstream work. */
    public static Decisions all(Collection<? extends Task<?>> tasks, Decision decision) {
        Builder builder = builder();
        tasks.forEach(task -> builder.decide(task, decision));
        return builder.build();
    }

    public Collection<Verdict> verdicts() {
        return verdicts.values();
    }

    public List<Task<?>> tasks() {
        return verdicts.values().stream().<Task<?>>map(Verdict::task).toList();
    }

    public boolean covers(Task<?> task) {
        return verdicts.containsKey(task.id());
    }

    public Decision decision(Task<?> task) {
        return verdict(task).decision();
    }

    public List<Push<?>> downstream(Task<?> task) {
        return verdict(task).downstream();
    }

    /** The tasks whose decision continues, so their downstream work is enqueued. */
    public List<Task<?>> continuing() {
        return verdicts.values().stream().filter(verdict -> verdict.decision().continues()).<Task<?>>map(Verdict::task).toList();
    }

    /** These decisions with one task's decision replaced. Its staged downstream work is kept. */
    public Decisions revise(Task<?> task, Decision decision) {
        Verdict current = verdict(task);
        Map<Long, Verdict> revised = new LinkedHashMap<>(verdicts);
        revised.put(task.id(), new Verdict(current.task(), decision, current.downstream(), current.junctions()));
        return new Decisions(revised);
    }

    /**
     * Combines two views of the same batch: per task, the more severe decision wins. The
     * downstream work and junctions come from this instance; a review can't add any.
     */
    public Decisions merge(Decisions other) {
        if (!verdicts.keySet().equals(other.verdicts.keySet())) {
            throw new IllegalArgumentException("Cannot merge decisions for different tasks: " + verdicts.keySet() + " and " + other.verdicts.keySet());
        }
        Map<Long, Verdict> merged = new LinkedHashMap<>();
        verdicts.forEach((id, verdict) -> merged.put(id, new Verdict(
            verdict.task(),
            Decision.merge(verdict.decision(), other.verdicts.get(id).decision()),
            verdict.downstream(),
            verdict.junctions()
        )));
        return new Decisions(merged);
    }

    private Verdict verdict(Task<?> task) {
        Verdict verdict = verdicts.get(task.id());
        if (verdict == null) {
            throw new IllegalArgumentException("No decision for task " + task.id());
        }
        return verdict;
    }

    @Override
    public String toString() {
        return verdicts.values().stream()
            .map(verdict -> verdict.task().id() + "=" + verdict.decision() + (verdict.downstream().isEmpty() ? "" : "+" + verdict.downstream().size()))
            .collect(Collectors.joining(", ", "Decisions[", "]"));
    }

    public static final class Builder {

        private final Map<Long, Verdict> verdicts = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder decide(Task<?> task, Decision decision) {
            return decide(task, decision, List.of());
        }

        public Builder decide(Task<?> task, Decision decision, List<Push<?>> downstream) {
            return decide(task, decision, downstream, List.of());
        }

        public Builder decide(Task<?> task, Decision decision, List<Push<?>> downstream, List<Junction> junctions) {
            if (verdicts.putIfAbsent(task.id(), new Verdict(task, decision, downstream, junctions)) != null) {
                throw new IllegalStateException("Task " + task.id() + " already has a decision");
            }
            return this;
        }

        public Builder decideAll(Collection<? extends Task<?>> tasks, Decision decision) {
            tasks.forEach(task -> decide(task, decision));
            return this;
        }

        public Set<Long> decided() {
            return Set.copyOf(verdicts.keySet());
        }

        public Decisions build() {
            return new Decisions(new LinkedHashMap<>(verdicts));
        }
    }
}
