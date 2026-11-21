package io.github.khopland.pgtasks.spring.slice;

import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.engine.Retry;
import io.github.khopland.pgtasks.engine.TaskConfigurer;
import io.github.khopland.pgtasks.engine.TaskGlobals;
import io.github.khopland.pgtasks.slice.AssessmentHandler;
import io.github.khopland.pgtasks.slice.AssessmentLedger;
import io.github.khopland.pgtasks.slice.FeedPublisher;
import io.github.khopland.pgtasks.slice.IncomeFeed;
import io.github.khopland.pgtasks.slice.NoticeHandler;
import io.github.khopland.pgtasks.slice.PartsRegister;
import io.github.khopland.pgtasks.slice.Slice;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.annotation.Transactional;

/**
 * The Phase 1 vertical slice as a Spring Boot application. The handlers, the feed, the ledger and
 * the publisher are the slice's own classes, unchanged; only the wiring is Spring's: beans, one
 * configurer for the slice's topics, and a transactional service that reads the feed.
 */
@SpringBootApplication
public class SliceApplication {

    static final Retry FAST = Retry.exponential(Duration.ofMillis(50), Duration.ofMillis(400)).attempts(5).withJitter(0.5);

    @Bean
    PartsRegister partsRegister() {
        return new PartsRegister().register("anna", 30).register("bjorn", 40);
    }

    @Bean
    AssessmentLedger assessmentLedger(DataSource dataSource) {
        return new AssessmentLedger(dataSource);
    }

    @Bean
    AssessmentHandler assessmentHandler(PartsRegister register, AssessmentLedger ledger) {
        return new AssessmentHandler(register, ledger);
    }

    @Bean
    FeedPublisher feedPublisher() {
        return new FeedPublisher();
    }

    @Bean
    TaskConfigurer sliceTasks(AssessmentHandler assessments, FeedPublisher publisher) {
        return tasks -> tasks
            .codec(Slice.AssessmentRequest.class, Slice.REQUESTS)
            .codec(Slice.Notice.class, Slice.NOTICES)
            .defaults(options -> options.retry(FAST))
            .topic(Slice.ASSESSMENT, assessments)
            .topic(Slice.NOTIFY, new NoticeHandler(publisher), options -> options.resources(publisher))
            .topic(Slice.ARCHIVE, batch -> batch.decide(task -> Decision.success()));
    }

    /** Retries are due within milliseconds, so promote often. */
    @Bean
    TaskGlobals taskSettings() {
        return globals -> globals.housekeeping(housekeeping -> housekeeping.promoteEvery(Duration.ofMillis(50)));
    }

    @Bean
    IncomeFeedJob incomeFeedJob(Tasks tasks, DataSource dataSource) {
        return new IncomeFeedJob(new IncomeFeed(tasks), dataSource);
    }

    /** Reads a page of the feed in a Spring transaction: the tasks and the pointer commit together. */
    public static class IncomeFeedJob {

        private final IncomeFeed feed;
        private final DataSource dataSource;

        IncomeFeedJob(IncomeFeed feed, DataSource dataSource) {
            this.feed = feed;
            this.dataSource = dataSource;
        }

        @Transactional
        public int readPage(int size) throws SQLException {
            return feed.readPage(DataSourceUtils.getConnection(dataSource), size);
        }

        /** Reads a page, then fails, as if the rest of the job had. */
        @Transactional
        public void readPageThenFail(int size) throws SQLException {
            feed.readPage(DataSourceUtils.getConnection(dataSource), size);
            throw new IllegalStateException("the feed job failed after reading its page");
        }
    }
}
