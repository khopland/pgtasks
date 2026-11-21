package io.github.khopland.pgtasks.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.ManualClock;
import org.junit.jupiter.api.Test;

/** Time windows on rate limits: lower in office hours, higher at night and at weekends. */
class RateWindowTest {

    private static final ZoneId OSLO = ZoneId.of("Europe/Oslo");
    private static final RateLimit OFFICE_HOURS = new RateLimit("register", 100, 10, List.of(
        new RateWindow(Cron.parse("0 7 * * MON-FRI", OSLO), Rate.perSecond(50)),
        new RateWindow(Cron.parse("0 17 * * MON-FRI", OSLO), Rate.perSecond(200))));

    private static double rate(RateLimit limit, String at) {
        return limit.at(Instant.parse(at)).perSecond();
    }

    @Test
    void each_window_holds_until_the_next_one_fires() {
        // Oslo is UTC+1 in November.
        assertThat(rate(OFFICE_HOURS, "2026-11-23T06:30:00Z")).as("Monday 07:30").isEqualTo(50);
        assertThat(rate(OFFICE_HOURS, "2026-11-23T15:59:00Z")).as("Monday 16:59").isEqualTo(50);
        assertThat(rate(OFFICE_HOURS, "2026-11-23T16:00:00Z")).as("Monday 17:00").isEqualTo(200);
        assertThat(rate(OFFICE_HOURS, "2026-11-21T12:00:00Z")).as("Saturday, since Friday 17:00").isEqualTo(200);
        assertThat(OFFICE_HOURS.at(Instant.parse("2026-11-23T06:30:00Z")).burst()).as("windows change the rate only").isEqualTo(10);
        assertThat(RateLimit.perSecond("plain", 100, 10).at(Instant.now()).perSecond()).isEqualTo(100);
    }

    @Test
    void the_engine_follows_the_clock_and_an_override_still_wins() throws Exception {
        ManualClock clock = new ManualClock(Instant.parse("2026-11-23T05:00:00Z"));
        try (TaskEngine engine = TaskEngine.builder()
            .store(new InMemoryTaskStore(clock))
            .globals(globals -> globals.limit("register", limit -> limit.rate(Rate.perSecond(100)).burst(10)
                .window(Cron.parse("0 7 * * MON-FRI", OSLO), Rate.perSecond(50))
                .window(Cron.parse("0 17 * * MON-FRI", OSLO), Rate.perSecond(200))))
            .configure("work", tasks -> tasks.topic("work", String.class, batch -> batch.decide(task -> Decision.success()),
                topic -> topic.limit("register")))
            .build()
            .start()) {
            assertThat(effective(engine)).as("Monday 06:00 in Oslo: since Friday 17:00").isEqualTo(200);

            clock.advance(Duration.ofHours(1));
            assertThat(effective(engine)).as("Monday 07:00").isEqualTo(50);
            assertThat(engine.limits().find("register").orElseThrow().configured().windows()).hasSize(2);

            engine.limits().override("register", Rate.perSecond(5), null, Duration.ofHours(1), "test", null);
            assertThat(effective(engine)).isEqualTo(5);
        }
    }

    private static double effective(TaskEngine engine) {
        return engine.limits().find("register").orElseThrow().effective().perSecond();
    }
}
