package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.temporal.TemporalUtil;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class OperationTypeMetricsManager
{
    private static final String METRIC_RUNTIME = "Runtime";

    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final ContinuousMetricManager runTimeMetric;
    private final String name;
    private final TimeUnit unit;
    private final long highestExpectedRuntimeDurationAsNano;
    private final LoggingService loggingService;

    OperationTypeMetricsManager(
            String name,
            TimeUnit unit,
            long highestExpectedRuntimeDurationAsNano,
            LoggingServiceFactory loggingServiceFactory )
    {
        this.name = name;
        this.unit = unit;
        this.highestExpectedRuntimeDurationAsNano = highestExpectedRuntimeDurationAsNano;
        loggingService = loggingServiceFactory.loggingServiceFor( getClass().getSimpleName() );
        this.runTimeMetric = new ContinuousMetricManager(
                METRIC_RUNTIME,
                unit,
                unit.convert( highestExpectedRuntimeDurationAsNano, TimeUnit.NANOSECONDS ),
                4
        );
    }

    void measure( long runDurationAsNano ) throws MetricsCollectionException
    {
        //
        // Measure operation runtime
        //
        if ( runDurationAsNano > highestExpectedRuntimeDurationAsNano )
        {
            String errMsg = format(
                    "Error recording runtime - reported value exceeds maximum allowed. Time " +
                    "reported as maximum.\n"
                    + "Reported: %s %s / %s\n"
                    + "For: %s\n"
                    + "Maximum: %s %s / %s",
                    runDurationAsNano,
                    TimeUnit.NANOSECONDS.name(),
                    temporalUtil.nanoDurationToString( runDurationAsNano ),
                    name,
                    highestExpectedRuntimeDurationAsNano,
                    TimeUnit.NANOSECONDS.name(),
                    temporalUtil.nanoDurationToString( highestExpectedRuntimeDurationAsNano )
            );
            loggingService.info( errMsg );
            runDurationAsNano = highestExpectedRuntimeDurationAsNano;
        }

        long runtimeInAppropriateUnit = unit.convert( runDurationAsNano, TimeUnit.NANOSECONDS );

        try
        {
            runTimeMetric.addMeasurement( runtimeInAppropriateUnit );
        }
        catch ( Throwable e )
        {
            String errMsg = format(
                    "Error encountered adding runtime: %s %s / %s %s\nTo: %s\nHighest expected value: %s %s / %s %s",
                    runDurationAsNano,
                    TimeUnit.NANOSECONDS.name(),
                    runtimeInAppropriateUnit,
                    unit.name(),
                    name,
                    highestExpectedRuntimeDurationAsNano,
                    TimeUnit.NANOSECONDS.name(),
                    unit.convert( highestExpectedRuntimeDurationAsNano, TimeUnit.NANOSECONDS ),
                    unit.name()
            );
            throw new MetricsCollectionException( errMsg, e );
        }
    }

    public OperationMetricsSnapshot snapshot()
    {
        return new OperationMetricsSnapshot( name, unit, count(), runTimeMetric.snapshot() );
    }

    public String name()
    {
        return name;
    }

    public long count()
    {
        return runTimeMetric.snapshot().count();
    }

    static class OperationMetricsNameComparator implements Comparator<OperationMetricsSnapshot>
    {
        private static final String EMPTY_STRING = "";

        @Override
        public int compare( OperationMetricsSnapshot metrics1, OperationMetricsSnapshot metrics2 )
        {
            String metrics1Name = (metrics1.name() == null) ? EMPTY_STRING : metrics1.name();
            String metrics2Name = (metrics2.name() == null) ? EMPTY_STRING : metrics2.name();
            return metrics1Name.compareTo( metrics2Name );
        }
    }
}
