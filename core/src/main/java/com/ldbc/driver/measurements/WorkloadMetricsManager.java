package com.ldbc.driver.measurements;

import java.util.ArrayList;
import java.util.List;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.util.Duration;
import com.ldbc.driver.util.MultipleTimeUnitProvider;
import com.ldbc.driver.util.TimeUnit;

public class WorkloadMetricsManager
{
    public static final Duration DEFAULT_HIGHEST_EXPECTED_DURATION = Duration.fromSeconds( 60 );
    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLI;

    private final MetricGroup runtimeMetrics;
    private final MetricGroup startTimeDelayMetrics;

    // TODO mean, stddev, percentile doesn't make sense
    // TODO use my Histogram for discrete things?
    // private final Map<String, OneMeasurement> resultCodeMeasurements;

    // TODO HdrHistogram only supports long values
    // TODO use my Histogram for discrete things?
    // private final Map<String, OneMeasurement> resultMeasurements;

    private final Duration highestExpectedDuration;
    private final TimeUnit timeUnit;

    public WorkloadMetricsManager()
    {
        this( DEFAULT_TIME_UNIT, DEFAULT_HIGHEST_EXPECTED_DURATION );
    }

    public WorkloadMetricsManager( TimeUnit timeUnit )
    {
        this( timeUnit, DEFAULT_HIGHEST_EXPECTED_DURATION );
    }

    public WorkloadMetricsManager( Duration highestExpectedDuration )
    {
        this( DEFAULT_TIME_UNIT, highestExpectedDuration );
    }

    public WorkloadMetricsManager( TimeUnit timeUnit, Duration highestExpectedDuration )
    {
        this.highestExpectedDuration = highestExpectedDuration;
        this.timeUnit = timeUnit;
        MetricFactory metricFactory = new HdrHistogramMetricFactory( this.highestExpectedDuration );
        runtimeMetrics = new MetricGroup( "Runtime", metricFactory );
        startTimeDelayMetrics = new MetricGroup( "Start Time Delay", metricFactory );

        // TODO
        // resultCodeMeasurements = new HashMap<String, OneMeasurement>();
        // resultMeasurements = new HashMap<String, OneMeasurement>();
    }

    public void measure( OperationResult operationResult )
    {
        // Measure operation runtime
        Metric operationRuntimeMetric = runtimeMetrics.getOrCreateMetric( operationResult.getOperationType() );
        long runtimeInAppropriateUnit = toMeasuredTimeUnit( operationResult.getRunTime() );
        operationRuntimeMetric.addMeasurement( runtimeInAppropriateUnit );

        // Measure driver performance - how close is it to target throughput
        Metric operationStartTimeDelayMetric = startTimeDelayMetrics.getOrCreateMetric( operationResult.getOperationType() );
        Duration startTimeDelay = Duration.durationBetween( operationResult.getScheduledStartTime(),
                operationResult.getActualStartTime() );
        long startTimeDelayInAppropriateUnit = toMeasuredTimeUnit( startTimeDelay );
        operationStartTimeDelayMetric.addMeasurement( startTimeDelayInAppropriateUnit );

        // TODO
        // getMeasurement( operationName, resultCodeMeasurements )...
        // getMeasurement( operationName, resultMeasurements )...
    }

    public MetricGroup[] getAllMeasurements()
    {
        List<MetricGroup> allMetricGroups = new ArrayList<MetricGroup>();
        allMetricGroups.add( runtimeMetrics );
        allMetricGroups.add( startTimeDelayMetrics );
        return allMetricGroups.toArray( new MetricGroup[allMetricGroups.size()] );
    }

    private long toMeasuredTimeUnit( MultipleTimeUnitProvider multipleTimeUnitProvider )
    {
        switch ( timeUnit )
        {
        case NANO:
            return multipleTimeUnitProvider.asNano();
        case MICRO:
            return multipleTimeUnitProvider.asMicro();
        case MILLI:
            return multipleTimeUnitProvider.asMilli();
        case SECOND:
            return multipleTimeUnitProvider.asSeconds();
        }
        throw new MetricException( "Unexpected error - unknown time unit" );
    }
}
