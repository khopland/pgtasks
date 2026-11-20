package org.example.task2.slice;

import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;

import org.example.task2.Batch;
import org.example.task2.BatchHandler;
import org.example.task2.Decision;
import org.example.task2.Decisions;
import org.example.task2.Work;

/** One batched lookup, one retry-safe change per task, then a notice downstream. */
public final class AssessmentHandler implements BatchHandler<Slice.AssessmentRequest> {

    private final PartsRegister register;
    private final AssessmentLedger ledger;
    private volatile Consumer<Work<Slice.AssessmentRequest>> afterChange = task -> { };

    public AssessmentHandler(PartsRegister register, AssessmentLedger ledger) {
        this.register = register;
        this.ledger = ledger;
    }

    /** Test hook: runs after the change was applied and before the notice is staged. */
    public AssessmentHandler afterChange(Consumer<Work<Slice.AssessmentRequest>> afterChange) {
        this.afterChange = afterChange;
        return this;
    }

    @Override
    public Decisions handle(Batch<Slice.AssessmentRequest> batch) {
        Map<String, PartsRegister.Party> parties = register.lookupAll(new HashSet<>(batch.identifiers()));
        return batch.decide(task -> {
            PartsRegister.Party party = parties.get(task.payload().person());
            if (party == null) {
                return Decision.filtered("unknown person");
            }
            long tax = task.payload().income() * party.bracket() / 100;
            // Already applied on an earlier attempt? Then the notice is still needed, so carry on.
            ledger.apply(task.idempotencyKey(), task.payload().person(), tax);
            afterChange.accept(task);
            task.push(Slice.NOTIFY, task.identifier(), new Slice.Notice(task.payload().person(), tax));
            return Decision.success();
        });
    }
}
