package io.github.khopland.pgtasks.engine;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A cron schedule in the usual five fields, read in a time zone: minute (0-59), hour (0-23),
 * day of month (1-31), month (1-12 or JAN-DEC) and day of week (0-7 or SUN-SAT; 0 and 7 are
 * Sunday). A field is {@code *}, a value, a range {@code a-b}, any of those with a step
 * ({@code 8-18/2}, or {@code *}&#47;{@code 15} for every 15), or a comma-separated list of them. As in classic cron, when both
 * day fields are restricted a day matches either. The shorthands {@code @hourly}, {@code @daily},
 * {@code @weekly}, {@code @monthly} and {@code @yearly} work too.
 *
 * <p>Daylight saving time: a time skipped when clocks go forward fires at the first instant
 * after the gap, and a time that happens twice when clocks go back fires once, the first time.
 *
 * <pre>
 * Cron.parse("0 2 * * *", ZoneId.of("Europe/Oslo"))   // 02:00 every night, Oslo time
 * Cron.parse("*&#47;15 7-17 * * MON-FRI")                  // every 15 minutes in office hours, UTC
 * </pre>
 */
public final class Cron {

    private static final Map<String, String> SHORTHANDS = Map.of(
        "@hourly", "0 * * * *",
        "@daily", "0 0 * * *",
        "@midnight", "0 0 * * *",
        "@weekly", "0 0 * * 0",
        "@monthly", "0 0 1 * *",
        "@yearly", "0 0 1 1 *",
        "@annually", "0 0 1 1 *");
    private static final List<String> MONTHS = List.of("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");
    private static final List<String> DAYS = List.of("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT");
    /** A schedule that fires less often than this never fires: 31 February, say. */
    private static final int YEARS_AHEAD = 8;

    private final String expression;
    private final ZoneId zone;
    private final BitSet minutes, hours, daysOfMonth, months, daysOfWeek;
    private final boolean anyDayOfMonth, anyDayOfWeek;

    private Cron(String expression, ZoneId zone) {
        this.expression = expression;
        this.zone = zone;
        String[] fields = SHORTHANDS.getOrDefault(expression.toLowerCase(Locale.ROOT), expression).trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException("A cron expression has five fields (minute hour day-of-month month day-of-week), not "
                + fields.length + ": '" + expression + "'");
        }
        minutes = field(fields[0], 0, 59, List.of(), "minute");
        hours = field(fields[1], 0, 23, List.of(), "hour");
        daysOfMonth = field(fields[2], 1, 31, List.of(), "day of month");
        months = field(fields[3], 1, 12, MONTHS, "month");
        BitSet weekdays = field(fields[4], 0, 7, DAYS, "day of week");
        if (weekdays.get(7)) {
            weekdays.set(0);
            weekdays.clear(7);
        }
        daysOfWeek = weekdays;
        anyDayOfMonth = fields[2].equals("*");
        anyDayOfWeek = fields[4].equals("*");
        if (next(Instant.EPOCH.plus(365 * 30, ChronoUnit.DAYS)) == null) {
            throw new IllegalArgumentException("The cron expression '" + expression + "' never fires");
        }
    }

    /** A schedule in UTC. */
    public static Cron parse(String expression) {
        return parse(expression, ZoneOffset.UTC);
    }

    public static Cron parse(String expression, ZoneId zone) {
        return new Cron(Objects.requireNonNull(expression, "expression").trim(), Objects.requireNonNull(zone, "zone"));
    }

    public String expression() {
        return expression;
    }

    public ZoneId zone() {
        return zone;
    }

    /** The first time after {@code after} that the schedule fires, or null if it never does. */
    public Instant next(Instant after) {
        LocalDateTime start = LocalDateTime.ofInstant(after, zone).truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
        LocalDate day = start.toLocalDate();
        LocalDate last = day.plusYears(YEARS_AHEAD);
        LocalTime from = start.toLocalTime();
        while (!day.isAfter(last)) {
            if (!months.get(day.getMonthValue())) {
                day = day.withDayOfMonth(1).plusMonths(1);
                from = LocalTime.MIDNIGHT;
                continue;
            }
            if (matches(day)) {
                for (int hour = from.getHour(); hour < 24; hour++) {
                    if (!hours.get(hour)) {
                        continue;
                    }
                    int firstMinute = hour == from.getHour() ? from.getMinute() : 0;
                    for (int minute = minutes.nextSetBit(firstMinute); minute >= 0 && minute < 60; minute = minutes.nextSetBit(minute + 1)) {
                        Instant fire = resolve(day.atTime(hour, minute));
                        if (fire.isAfter(after)) {
                            return fire;
                        }
                    }
                }
            }
            day = day.plusDays(1);
            from = LocalTime.MIDNIGHT;
        }
        return null;
    }

    /** The last time at or before {@code atOrBefore} that the schedule fired, or null if it didn't within years. */
    public Instant previous(Instant atOrBefore) {
        LocalDateTime start = LocalDateTime.ofInstant(atOrBefore, zone).truncatedTo(ChronoUnit.MINUTES);
        LocalDate day = start.toLocalDate();
        LocalDate first = day.minusYears(YEARS_AHEAD);
        LocalTime until = start.toLocalTime();
        while (!day.isBefore(first)) {
            if (!months.get(day.getMonthValue())) {
                day = day.withDayOfMonth(1).minusDays(1);
                until = LocalTime.MAX;
                continue;
            }
            if (matches(day)) {
                for (int hour = until.getHour(); hour >= 0; hour--) {
                    if (!hours.get(hour)) {
                        continue;
                    }
                    int lastMinute = hour == until.getHour() ? until.getMinute() : 59;
                    for (int minute = minutes.previousSetBit(lastMinute); minute >= 0; minute = minutes.previousSetBit(minute - 1)) {
                        Instant fire = resolve(day.atTime(hour, minute));
                        if (!fire.isAfter(atOrBefore)) {
                            return fire;
                        }
                    }
                }
            }
            day = day.minusDays(1);
            until = LocalTime.MAX;
        }
        return null;
    }

    /** A local time in a gap fires right after it; one that happens twice fires the first time. */
    private Instant resolve(LocalDateTime local) {
        java.time.zone.ZoneRules rules = zone.getRules();
        if (rules.getValidOffsets(local).isEmpty()) {
            return rules.getTransition(local).getInstant();
        }
        return ZonedDateTime.ofLocal(local, zone, null).withEarlierOffsetAtOverlap().toInstant();
    }

    private boolean matches(LocalDate day) {
        boolean dayOfMonth = daysOfMonth.get(day.getDayOfMonth());
        boolean dayOfWeek = daysOfWeek.get(day.getDayOfWeek() == DayOfWeek.SUNDAY ? 0 : day.getDayOfWeek().getValue());
        if (anyDayOfMonth || anyDayOfWeek) {
            return dayOfMonth && dayOfWeek;
        }
        return dayOfMonth || dayOfWeek;
    }

    private static BitSet field(String field, int min, int max, List<String> names, String what) {
        BitSet values = new BitSet(max + 1);
        for (String part : field.split(",", -1)) {
            String range = part;
            int step = 1;
            int slash = part.indexOf('/');
            if (slash >= 0) {
                range = part.substring(0, slash);
                step = number(part.substring(slash + 1), 1, max, List.of(), what);
            }
            int from, to;
            if (range.equals("*")) {
                from = min;
                to = max;
            } else if (range.contains("-")) {
                String[] ends = range.split("-", -1);
                if (ends.length != 2) {
                    throw invalid(part, what);
                }
                from = number(ends[0], min, max, names, what);
                to = number(ends[1], min, max, names, what);
                if (from > to) {
                    throw new IllegalArgumentException("The " + what + " range '" + part + "' runs backwards");
                }
            } else {
                from = number(range, min, max, names, what);
                to = slash >= 0 ? max : from;
            }
            for (int value = from; value <= to; value += step) {
                values.set(value);
            }
        }
        return values;
    }

    private static int number(String text, int min, int max, List<String> names, String what) {
        int named = names.indexOf(text.toUpperCase(Locale.ROOT));
        if (named >= 0) {
            return named + (min == 1 ? 1 : 0);
        }
        int value;
        try {
            value = Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw invalid(text, what);
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException("The " + what + " " + value + " is outside " + min + "-" + max);
        }
        return value;
    }

    private static IllegalArgumentException invalid(String text, String what) {
        return new IllegalArgumentException("Not a valid " + what + ": '" + text + "'");
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Cron cron && cron.expression.equals(expression) && cron.zone.equals(zone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, zone);
    }

    @Override
    public String toString() {
        return expression + (zone.equals(ZoneOffset.UTC) ? "" : " (" + zone + ")");
    }
}
