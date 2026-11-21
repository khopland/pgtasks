package org.example.task2.spring.slice;

import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.example.task2.Decision;
import org.example.task2.Tasks;
import org.example.task2.engine.Retry;
import org.example.task2.engine.TaskConfigurer;
import org.example.task2.engine.TaskGlobals;
import org.example.task2.slice.AssessmentHandler;
import org.example.task2.slice.AssessmentLedger;
import org.example.task2.slice.FeedPublisher;
import org.example.task2.slice.IncomeFeed;
import org.example.task2.slice.NoticeHandler;
import org.example.task2.slice.PartsRegister;
import org.example.task2.slice.Slice;
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

    static final Retry FAST = new Retry(5, Duration.ofMillis(50), 2, Duration.ofMillis(400), 0.5);

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
