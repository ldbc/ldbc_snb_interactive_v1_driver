package com.ldbc.driver.temporal;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class Time implements Comparable<Time>, MultipleTimeUnitProvider<Time> {
    private static final SimpleDateFormat TIME_FORMAT;
    private static final SimpleDateFormat DATE_TIME_FORMAT;

    static {
        TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
        TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("GCT"));
        DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd - HH:mm:ss:SSS");
        DATE_TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public static Time fromNano(long ns) {
        return new Time(ns);
    }

    public static Time fromMicro(long us) {
        return fromNano(Temporal.convert(us, TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS));
    }

    public static Time fromMilli(long ms) {
        return fromNano(Temporal.convert(ms, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS));
    }

    public static Time fromSeconds(long s) {
        return fromNano(Temporal.convert(s, TimeUnit.SECONDS, TimeUnit.NANOSECONDS));
    }

    public static Time fromMinutes(long m) {
        return fromNano(Temporal.convert(m, TimeUnit.MINUTES, TimeUnit.NANOSECONDS));
    }

    public static Time from(TimeUnit timeUnit, long unitOfTime) {
        return fromNano(Temporal.convert(unitOfTime, timeUnit, TimeUnit.NANOSECONDS));
    }

    public static Time max(Time time1, Time time2) {
        return (time1.gt(time2)) ? time1 : time2;
    }

    private final Temporal time;

    private Time(long timeNano) {
        this.time = Temporal.fromNano(timeNano);
    }

    @Override
    public String toString() {
        return timeString();
    }

    public String timeString() {
        return TIME_FORMAT.format(new Date(time.asMilli()));
    }

    public String dateTimeString() {
        return DATE_TIME_FORMAT.format(new Date(time.asMilli()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (time.asNano() ^ (time.asNano() >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Time other = (Time) obj;
        return time.equals(other.time);
    }

    @Override
    public int compareTo(Time other) {
        if (this.time.asNano() == other.time.asNano()) return 0;
        return this.time.asNano() < other.time.asNano() ? -1 : 1;
    }

    @Override
    public long asNano() {
        return time.asNano();
    }

    @Override
    public long asMicro() {
        return time.asMicro();
    }

    @Override
    public long asMilli() {
        return time.asMilli();
    }

    @Override
    public long asSeconds() {
        return time.asSeconds();
    }

    @Override
    public long as(TimeUnit timeUnit) {
        return time.as(timeUnit);
    }

    @Override
    public boolean gt(Time other) {
        return time.gt(other.time);
    }

    @Override
    public boolean lt(Time other) {
        return time.lt(other.time);
    }

    @Override
    public boolean gte(Time other) {
        return time.gte(other.time);
    }

    @Override
    public boolean lte(Time other) {
        return time.lte(other.time);
    }

    @Override
    public Duration durationGreaterThan(Time other) {
        return time.durationGreaterThan(other.time);
    }

    @Override
    public Duration durationLessThan(Time other) {
        return time.durationLessThan(other.time);
    }

    @Override
    public Time plus(Duration duration) {
        return new Time(time.plus(duration).asNano());
    }

    @Override
    public Time minus(Duration duration) {
        return new Time(time.minus(duration).asNano());
    }
}
