package io.github.khopland.pgtasks.slice;

import io.github.khopland.pgtasks.PayloadCodec;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.test.PostgresDatabase;

/**
 * The vertical slice from roadmap phase 1: an income feed enqueues assessments in the same
 * commit as its pointer; the assessment topic does one batched lookup and one retry-safe change
 * per task, then pushes a notice; the notify topic stages a JDBC feed publication.
 */
public final class Slice {

    public record AssessmentRequest(String person, long income) {
    }

    public record Notice(String person, long tax) {
    }

    public static final TopicRef<AssessmentRequest> ASSESSMENT = TopicRef.of("assessment", AssessmentRequest.class);
    public static final TopicRef<Notice> NOTIFY = TopicRef.of("notify", Notice.class);
    public static final TopicRef<String> ARCHIVE = TopicRef.of("archive", String.class);

    public static final PayloadCodec<AssessmentRequest> REQUESTS = PayloadCodec.of(
        request -> request.person() + "|" + request.income(),
        text -> new AssessmentRequest(text.substring(0, text.indexOf('|')), Long.parseLong(text.substring(text.indexOf('|') + 1)))
    );

    public static final PayloadCodec<Notice> NOTICES = PayloadCodec.of(
        notice -> notice.person() + "|" + notice.tax(),
        text -> new Notice(text.substring(0, text.indexOf('|')), Long.parseLong(text.substring(text.indexOf('|') + 1)))
    );

    private Slice() {
    }

    /** The application's own tables, next to the pgtasks schema. */
    public static void createDomainTables(PostgresDatabase database) {
        database.execute("""
            CREATE TABLE incoming (position bigserial PRIMARY KEY, person text NOT NULL, income bigint NOT NULL);
            CREATE TABLE feed_pointer (name text PRIMARY KEY, position bigint NOT NULL);
            INSERT INTO feed_pointer VALUES ('income', 0);
            CREATE TABLE assessment (idempotency_key text PRIMARY KEY, person text NOT NULL, tax bigint NOT NULL);
            CREATE TABLE published_feed (position bigserial PRIMARY KEY, person text NOT NULL, notice text NOT NULL, source_task bigint NOT NULL);
            """);
    }
}
