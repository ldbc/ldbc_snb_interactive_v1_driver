package com.ldbc.driver.util.temporal;

public class DurationMeasurement
{
    public static DurationMeasurement startMeasurementNow()
    {
        return new DurationMeasurement( System.nanoTime() );
    }

    private final long startTimeNano;

    private DurationMeasurement( long startTimeNano )
    {
        this.startTimeNano = startTimeNano;
    }

    public Duration getDurationUntilNow()
    {
        return Duration.fromNano( System.nanoTime() - startTimeNano );
    }
}
