package com.ldbc.driver.temporal;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class TemporalUtil {
    private final SimpleDateFormat timeFormat;
    private final SimpleDateFormat dateTimeFormat;

    public TemporalUtil() {
        timeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
        timeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd - HH:mm:ss:SSS");
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public long convert(long unitOfTime, TimeUnit timeUnitFrom, TimeUnit timeUnitTo) throws TemporalException {
        long unitOfTimeInNewUnit = timeUnitTo.convert(unitOfTime, timeUnitFrom);
        if (unitOfTimeInNewUnit == Long.MIN_VALUE || unitOfTimeInNewUnit == Long.MAX_VALUE) {
            throw new TemporalException(
                    String.format("Overflow while converting %s %s to %s", unitOfTime, timeUnitFrom, timeUnitTo));
        }
        return unitOfTimeInNewUnit;
    }

    public String millisecondsToTimeString(long timeAsMilli) {
        return timeFormat.format(new Date(timeAsMilli));
    }

    public String millisecondsToDateTimeString(long timeAsMilli) {
        return dateTimeFormat.format(new Date(timeAsMilli));
    }

    public long millisecondsFromTimeString(String timeString) throws TemporalException {
        try {
            return timeFormat.parse(timeString).getTime();
        } catch (ParseException e) {
            throw new TemporalException(String.format("Unable to parse: %s", timeString), e);
        }
    }

    public long millisecondsFromDateTimeString(String dateTimeString) throws TemporalException {
        try {
            return dateTimeFormat.parse(dateTimeString).getTime();
        } catch (ParseException e) {
            throw new TemporalException(String.format("Unable to parse: %s", dateTimeString), e);
        }
    }

    public String milliDurationToString(long durationAsMilli) {
        return nanoDurationToString(convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));
    }

    public String nanoDurationToString(long durationAsNano) {
        long h = TimeUnit.NANOSECONDS.toHours(durationAsNano);
        if (h > 0) {
            long m = TimeUnit.NANOSECONDS.toMinutes(durationAsNano) - TimeUnit.HOURS.toMinutes(h);
            long s = TimeUnit.NANOSECONDS.toSeconds(durationAsNano) - TimeUnit.HOURS.toSeconds(h) - TimeUnit.MINUTES.toSeconds(m);
            long ms = TimeUnit.NANOSECONDS.toMillis(durationAsNano) - TimeUnit.HOURS.toMillis(h) - TimeUnit.MINUTES.toMillis(m) - TimeUnit.SECONDS.toMillis(s);
            long us = TimeUnit.NANOSECONDS.toMicros(durationAsNano) - TimeUnit.HOURS.toMicros(h) - TimeUnit.MINUTES.toMicros(m) - TimeUnit.SECONDS.toMicros(s) - TimeUnit.MILLISECONDS.toMicros(ms);
            return String.format("%02d:%02d:%02d.%03d.%03d (h:m:s.ms.us)", h, m, s, ms, us);
        } else {
            long m = TimeUnit.NANOSECONDS.toMinutes(durationAsNano);
            long s = TimeUnit.NANOSECONDS.toSeconds(durationAsNano) - TimeUnit.MINUTES.toSeconds(m);
            long ms = TimeUnit.NANOSECONDS.toMillis(durationAsNano) - TimeUnit.MINUTES.toMillis(m) - TimeUnit.SECONDS.toMillis(s);
            long us = TimeUnit.NANOSECONDS.toMicros(durationAsNano) - TimeUnit.MINUTES.toMicros(m) - TimeUnit.SECONDS.toMicros(s) - TimeUnit.MILLISECONDS.toMicros(ms);
            return String.format("%02d:%02d.%03d.%03d (m:s.ms.us)", m, s, ms, us);
        }
    }
}
