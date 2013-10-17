package com.ldbc.driver.util.temporal;

public class DurationMeasurement
{
    public static DurationMeasurement startMeasurementNow()
    {
        return new DurationMeasurement( Time.nowAsMilli() );
    }

    private final Time startTime;

    private DurationMeasurement( long startTimeMilli )
    {
        this.startTime = Time.fromMilli( startTimeMilli );
    }

    public Time startTime()
    {
        return startTime;
    }

    public Duration durationUntilNow()
    {
        return Duration.fromMilli( Time.nowAsMilli() - startTime.asMilli() );
    }
}
