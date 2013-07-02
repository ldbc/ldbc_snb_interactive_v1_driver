package com.ldbc.driver.measurements;

import java.util.ArrayList;
import java.util.List;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.util.time.Duration;
import com.ldbc.driver.util.time.MultipleTimeUnitProvider;
import com.ldbc.driver.util.time.Time;
import com.ldbc.driver.util.time.TimeUnit;

public class WorkloadMetricsManager
{
    public static final String METRIC_RUNTIME = "Runtime";
    public static final String METRIC_START_TIME = "Start Time";
    public static final String METRIC_START_TIME_DELAY = "Start Time Delay";
    public static final String METRIC_RESULT_CODE = "Result Code";

    private static final Duration MINUTES_1 = Duration.fromSeconds( 60 );
    private static final Duration MINUTES_15 = Duration.fromSeconds( 60 * 15 );

    public static final Duration DEFAULT_HIGHEST_EXPECTED_DURATION = MINUTES_1;
    public static final Time DEFAULT_HIGHEST_EXPECTED_TIME = Time.now().plus( MINUTES_15 );
    public static final TimeUnit DEFAULT_DURATION_UNIT = TimeUnit.NANO;
    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLI;

    private final MetricGroup runtimeMetrics;
    private final MetricGroup startTimeMetrics;
    private final MetricGroup startTimeDelayMetrics;
    private final MetricGroup resultCodeMetrics;

    private final Duration highestExpectedDuration;
    private final Time highestExpectedTime;
    private final TimeUnit timeUnit;
    private final TimeUnit durationUnit;

    private Time workloadEndTime = Time.now();

    public WorkloadMetricsManager()
    {
        this( DEFAULT_TIME_UNIT, DEFAULT_DURATION_UNIT, DEFAULT_HIGHEST_EXPECTED_TIME,
                DEFAULT_HIGHEST_EXPECTED_DURATION );
    }

    public WorkloadMetricsManager( TimeUnit timeUnit, TimeUnit durationUnit )
    {
        this( timeUnit, durationUnit, DEFAULT_HIGHEST_EXPECTED_TIME, DEFAULT_HIGHEST_EXPECTED_DURATION );
    }

    public WorkloadMetricsManager( TimeUnit timeUnit, TimeUnit durationUnit, Time highestExpectedTime,
            Duration highestExpectedDuration )
    {
        this.highestExpectedDuration = highestExpectedDuration;
        this.highestExpectedTime = highestExpectedTime;
        this.timeUnit = timeUnit;
        this.durationUnit = durationUnit;
        MetricFactory durationMetricFactory = new HdrHistogramMetricFactory( toMeasuredUnit( this.durationUnit,
                this.highestExpectedDuration ) );
        MetricFactory timeMetricFactory = new HdrHistogramMetricFactory( toMeasuredUnit( this.timeUnit,
                this.highestExpectedTime ) );
        MetricFactory resultCodeMetricFactory = new HdrHistogramMetricFactory( 1000 );
        runtimeMetrics = new MetricGroup( METRIC_RUNTIME, durationMetricFactory );
        startTimeMetrics = new MetricGroup( METRIC_START_TIME, timeMetricFactory );
        startTimeDelayMetrics = new MetricGroup( METRIC_START_TIME_DELAY, durationMetricFactory );
        resultCodeMetrics = new MetricGroup( METRIC_RESULT_CODE, resultCodeMetricFactory );
    }

    public void measure( OperationResult operationResult )
    {
        // Measure end of workload
        Time operationEndTime = operationResult.getActualStartTime().plus( operationResult.getRunTime() );
        if ( workloadEndTime.asNano() < operationEndTime.asNano() )
        {
            workloadEndTime = operationEndTime;
        }

        // Measure operation runtime
        Metric operationRuntimeMetric = runtimeMetrics.getOrCreateMetric( operationResult.getOperationType() );
        long runtimeInAppropriateUnit = toMeasuredUnit( durationUnit, operationResult.getRunTime() );
        operationRuntimeMetric.addMeasurement( runtimeInAppropriateUnit );

        // Measure all start times
        Metric operationStartTimeMetric = startTimeMetrics.getOrCreateMetric( operationResult.getOperationType() );
        long startTimeInAppropriateUnit = toMeasuredUnit( timeUnit, operationResult.getActualStartTime() );
        operationStartTimeMetric.addMeasurement( startTimeInAppropriateUnit );

        // Measure driver performance - how close is it to target throughput
        Metric operationStartTimeDelayMetric = startTimeDelayMetrics.getOrCreateMetric( operationResult.getOperationType() );
        Duration startTimeDelay = Duration.durationBetween( operationResult.getScheduledStartTime(),
                operationResult.getActualStartTime() );
        long startTimeDelayInAppropriateUnit = toMeasuredUnit( durationUnit, startTimeDelay );
        operationStartTimeDelayMetric.addMeasurement( startTimeDelayInAppropriateUnit );

        // Measure result code
        Metric operationResultCodeMetric = resultCodeMetrics.getOrCreateMetric( operationResult.getOperationType() );
        int operationResultCode = operationResult.getResultCode();
        operationResultCodeMetric.addMeasurement( operationResultCode );
    }

    public MetricGroup[] getAllMeasurements()
    {
        List<MetricGroup> allMetricGroups = new ArrayList<MetricGroup>();
        allMetricGroups.add( runtimeMetrics );
        allMetricGroups.add( startTimeMetrics );
        allMetricGroups.add( startTimeDelayMetrics );
        allMetricGroups.add( resultCodeMetrics );
        return allMetricGroups.toArray( new MetricGroup[allMetricGroups.size()] );
    }

    public Metric getRuntimeMeasurementsFor( String operationName )
    {
        return runtimeMetrics.getMetric( operationName );
    }

    public Metric getStartTimeMeasurementsFor( String operationName )
    {
        return startTimeMetrics.getMetric( operationName );
    }

    public Metric getStartTimeDelayMeasurementsFor( String operationName )
    {
        return startTimeDelayMetrics.getMetric( operationName );
    }

    public Metric getResultCodeMeasurementsFor( String operationName )
    {
        return resultCodeMetrics.getMetric( operationName );
    }

    public Time getWorkloadStartTime()
    {
        long workloadStartTime = Long.MAX_VALUE;
        for ( String operationName : getAllMeasuredOperationTypes() )
        {
            Metric operationStartTimeMetrics = startTimeMetrics.getMetric( operationName );
            workloadStartTime = Math.min( workloadStartTime, operationStartTimeMetrics.getMin() );
        }
        return timeFromMeasuredUnit( timeUnit, workloadStartTime );
    }

    public Time getWorkloadEndTime()
    {
        return workloadEndTime;
    }

    public String[] getAllMeasuredOperationTypes()
    {
        List<String> allOperationTypes = new ArrayList<String>();
        // Could just as well use any other MetricsGroup
        for ( Metric metric : runtimeMetrics.getMetrics() )
        {
            allOperationTypes.add( metric.getName() );
        }

        return allOperationTypes.toArray( new String[allOperationTypes.size()] );
    }

    private long toMeasuredUnit( TimeUnit unit, MultipleTimeUnitProvider multipleTimeUnitProvider )
    {
        switch ( unit )
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

    private Time timeFromMeasuredUnit( TimeUnit unit, long measurement )
    {
        switch ( unit )
        {
        case NANO:
            return Time.fromNano( measurement );
        case MICRO:
            return Time.fromMicro( measurement );
        case MILLI:
            return Time.fromMilli( measurement );
        case SECOND:
            return Time.fromSeconds( measurement );
        }
        throw new MetricException( "Unexpected error - unknown time unit" );
    }
}
