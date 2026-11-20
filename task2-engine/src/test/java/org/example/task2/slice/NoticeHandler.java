package org.example.task2.slice;

import org.example.task2.Batch;
import org.example.task2.BatchHandler;
import org.example.task2.Decision;
import org.example.task2.Decisions;

/** Publishes every notice to the feed and archives it. */
public final class NoticeHandler implements BatchHandler<Slice.Notice> {

    private final FeedPublisher publisher;

    public NoticeHandler(FeedPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public Decisions handle(Batch<Slice.Notice> batch) {
        FeedPublisher.Staging staging = batch.resource(publisher);
        return batch.decide(task -> {
            staging.publish(task.task(), task.payload().person(), "tax " + task.payload().tax());
            task.push(Slice.ARCHIVE, task.identifier(), "notice " + task.id());
            return Decision.success();
        });
    }
}
