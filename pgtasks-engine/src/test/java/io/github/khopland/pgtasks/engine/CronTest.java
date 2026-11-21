package io.github.khopland.pgtasks.engine;

import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CronTest {

    private static final ZoneId OSLO = ZoneId.of("Europe/Oslo");

    private static Instant at(String instant) {
        return Instant.parse(instant);
    }

    @Test
    void every_fifteen_minutes_in_office_hours_on_weekdays() {
        Cron cron = Cron.parse("*/15 7-17 * * MON-FRI");
        // Friday 2026-11-20, 17:50 UTC: the next is Monday 07:00.
        assertThat(cron.next(at("2026-11-20T17:50:00Z"))).isEqualTo(at("2026-11-23T07:00:00Z"));
        assertThat(cron.next(at("2026-11-23T07:00:00Z"))).isEqualTo(at("2026-11-23T07:15:00Z"));
        assertThat(cron.next(at("2026-11-23T07:14:59Z"))).isEqualTo(at("2026-11-23T07:15:00Z"));
    }

    @Test
    void lists_ranges_steps_names_and_shorthands() {
        assertThat(Cron.parse("0 9,13 * * *").next(at("2026-11-21T09:00:00Z"))).isEqualTo(at("2026-11-21T13:00:00Z"));
        assertThat(Cron.parse("30 8-18/5 * * *").next(at("2026-11-21T09:00:00Z"))).isEqualTo(at("2026-11-21T13:30:00Z"));
        assertThat(Cron.parse("0 0 1 jan,jul *").next(at("2026-11-21T00:00:00Z"))).isEqualTo(at("2027-01-01T00:00:00Z"));
        assertThat(Cron.parse("@daily").next(at("2026-11-21T10:00:00Z"))).isEqualTo(at("2026-11-22T00:00:00Z"));
        assertThat(Cron.parse("@weekly").next(at("2026-11-21T10:00:00Z"))).as("Sunday").isEqualTo(at("2026-11-22T00:00:00Z"));
        assertThat(Cron.parse("0 0 * * 7").next(at("2026-11-21T10:00:00Z"))).as("7 is Sunday too").isEqualTo(at("2026-11-22T00:00:00Z"));
    }

    @Test
    void with_both_day_fields_restricted_either_matches() {
        // The 1st of the month, or any Monday: Monday 23 November comes first.
        assertThat(Cron.parse("0 0 1 * MON").next(at("2026-11-21T00:00:00Z"))).isEqualTo(at("2026-11-23T00:00:00Z"));
        assertThat(Cron.parse("0 0 1 * MON").next(at("2026-11-30T00:00:00Z"))).isEqualTo(at("2026-12-01T00:00:00Z"));
    }

    @Test
    void a_zone_moves_the_schedule_and_daylight_saving_is_handled() {
        Cron nightly = Cron.parse("0 2 * * *", OSLO);
        assertThat(nightly.next(at("2026-11-21T00:00:00Z"))).as("02:00 in Oslo is 01:00 UTC in winter").isEqualTo(at("2026-11-21T01:00:00Z"));
        // 29 March 2026: clocks go from 02:00 to 03:00, so 02:30 doesn't exist and fires at 03:00 local.
        assertThat(Cron.parse("30 2 * * *", OSLO).next(at("2026-03-28T12:00:00Z"))).isEqualTo(at("2026-03-29T01:00:00Z"));
        // 25 October 2026: 02:30 happens twice; it fires once, the first time (CEST, 00:30 UTC).
        Cron twice = Cron.parse("30 2 * * *", OSLO);
        Instant first = twice.next(at("2026-10-24T12:00:00Z"));
        assertThat(first).isEqualTo(at("2026-10-25T00:30:00Z"));
        assertThat(twice.next(first)).isEqualTo(at("2026-10-26T01:30:00Z"));
    }

    @Test
    void a_leap_day_is_found_years_ahead() {
        assertThat(Cron.parse("0 0 29 2 *").next(at("2026-11-21T00:00:00Z"))).isEqualTo(at("2028-02-29T00:00:00Z"));
    }

    @Test
    void previous_finds_the_last_fire_at_or_before() {
        Cron cron = Cron.parse("*/15 7-17 * * MON-FRI");
        assertThat(cron.previous(at("2026-11-23T07:15:00Z"))).as("at counts").isEqualTo(at("2026-11-23T07:15:00Z"));
        assertThat(cron.previous(at("2026-11-23T07:14:59Z"))).isEqualTo(at("2026-11-23T07:00:00Z"));
        // Monday 06:00: the last was Friday 17:45.
        assertThat(cron.previous(at("2026-11-23T06:00:00Z"))).isEqualTo(at("2026-11-20T17:45:00Z"));
        assertThat(Cron.parse("0 0 29 2 *").previous(at("2026-11-21T00:00:00Z"))).isEqualTo(at("2024-02-29T00:00:00Z"));
        assertThat(Cron.parse("0 2 * * *", OSLO).previous(at("2026-11-21T00:59:00Z"))).isEqualTo(at("2026-11-20T01:00:00Z"));
    }

    @Test
    void mistakes_are_explained() {
        assertThatThrownBy(() -> Cron.parse("0 2 * *")).hasMessageContaining("five fields");
        assertThatThrownBy(() -> Cron.parse("61 * * * *")).hasMessageContaining("minute 61 is outside 0-59");
        assertThatThrownBy(() -> Cron.parse("0 0 * * FUN")).hasMessageContaining("day of week");
        assertThatThrownBy(() -> Cron.parse("0 0 31 2 *")).hasMessageContaining("never fires");
        assertThatThrownBy(() -> Cron.parse("0 5-1 * * *")).hasMessageContaining("backwards");
    }
}
