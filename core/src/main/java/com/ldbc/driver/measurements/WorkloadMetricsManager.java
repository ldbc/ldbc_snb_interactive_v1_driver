package com.ldbc.driver.measurements;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;
import com.ldbc.driver.util.temporal.TimeUnit;

public class WorkloadMetricsManager
{
    private static Logger logger = Logger.getLogger( WorkloadMetricsManager.class );

    public static final String METRIC_RUNTIME = "Runtime";
    public static final String METRIC_START_TIME_DELAY = "Start Time Delay";
    public static final String METRIC_RESULT_CODE = "Result Code";

    private static final Duration MINUTES_10 = Duration.fromSeconds( 60 * 10 );
    private static final Duration MINUTES_60 = Duration.fromSeconds( 60 * 60 );

    public static final Duration DEFAULT_HIGHEST_EXPECTED_DURATION = MINUTES_10;
    public static final TimeUnit DEFAULT_DURATION_UNIT = TimeUnit.MICRO;

    private final MetricGroup runtimeMetrics;
    private final MetricGroup startTimeDelayMetrics;
    private final MetricGroup resultCodeMetrics;

    private final TimeUnit durationUnit;

    private Time workloadStartTime = Time.now().plus( MINUTES_60 );
    private Time workloadEndTime = Time.now();

    public WorkloadMetricsManager()
    {
        this( DEFAULT_DURATION_UNIT, DEFAULT_HIGHEST_EXPECTED_DURATION );
    }

    public WorkloadMetricsManager( TimeUnit durationUnit )
    {
        this( durationUnit, DEFAULT_HIGHEST_EXPECTED_DURATION );
    }

    public WorkloadMetricsManager( TimeUnit durationUnit, Duration highestExpectedDuration )
    {
        this.durationUnit = durationUnit;
        MetricFactory durationMetricFactory = new HdrHistogramMetricFactory(
                highestExpectedDuration.as( this.durationUnit ), 5 );
        MetricFactory resultCodeMetricFactory = new HdrHistogramMetricFactory( 1000, 5 );
        runtimeMetrics = new MetricGroup( METRIC_RUNTIME, durationMetricFactory );
        startTimeDelayMetrics = new MetricGroup( METRIC_START_TIME_DELAY, durationMetricFactory );
        resultCodeMetrics = new MetricGroup( METRIC_RESULT_CODE, resultCodeMetricFactory );
    }

    public void measure( OperationResult operationResult )
    {
        //
        // Measure start of workload
        //
        Time operationStartTime = operationResult.getActualStartTime();
        if ( workloadStartTime.asNano() > operationStartTime.asNano() )
        {
            workloadStartTime = operationStartTime;
        }

        //
        // Measure end of workload
        //
        Time operationEndTime = operationResult.getActualStartTime().plus( operationResult.getRunTime() );
        if ( workloadEndTime.asNano() < operationEndTime.asNano() )
        {
            workloadEndTime = operationEndTime;
        }

        //
        // Measure operation runtime
        //
        Metric operationRuntimeMetric = runtimeMetrics.getOrCreateMetric( operationResult.getOperationType() );
        long runtimeInAppropriateUnit = operationResult.getRunTime().as( durationUnit );
        try
        {
            operationRuntimeMetric.addMeasurement( runtimeInAppropriateUnit );
        }
        catch ( MetricException e )
        {
            String errMsg = String.format( "Error encountered adding Run Time measurement [%s]",
                    runtimeInAppropriateUnit );
            logger.error( errMsg, e );
            throw new MetricException( errMsg, e.getCause() );
        }

        //
        // Measure driver performance - how close is it to target throughput
        //
        Metric operationStartTimeDelayMetric = startTimeDelayMetrics.getOrCreateMetric( operationResult.getOperationType() );
        Duration startTimeDelay = Duration.durationBetween( operationResult.getScheduledStartTime(),
                operationResult.getActualStartTime() );
        long startTimeDelayInAppropriateUnit = startTimeDelay.as( durationUnit );
        try
        {
            operationStartTimeDelayMetric.addMeasurement( startTimeDelayInAppropriateUnit );
        }
        catch ( MetricException e )
        {
            String errMsg = String.format( "Error encountered adding Time Delay measurement [%s]",
                    startTimeDelayInAppropriateUnit );
            logger.error( errMsg, e );
            throw new MetricException( errMsg, e.getCause() );
        }

        //
        // Measure result code
        //
        Metric operationResultCodeMetric = resultCodeMetrics.getOrCreateMetric( operationResult.getOperationType() );
        int operationResultCode = operationResult.getResultCode();
        try
        {
            operationResultCodeMetric.addMeasurement( operationResultCode );
        }
        catch ( MetricException e )
        {
            String errMsg = String.format( "Error encountered adding Result Code measurement [%s]", operationResultCode );
            logger.error( errMsg, e );
            throw new MetricException( errMsg, e.getCause() );
        }
    }

    public MetricGroup[] getAllMeasurements()
    {
        List<MetricGroup> allMetricGroups = new ArrayList<MetricGroup>();
        allMetricGroups.add( runtimeMetrics );
        allMetricGroups.add( startTimeDelayMetrics );
        // TODO rather than returning all measurements this class should format
        // and export itself
        // allMetricGroups.add( resultCodeMetrics );
        return allMetricGroups.toArray( new MetricGroup[allMetricGroups.size()] );
    }

    public Metric getRuntimeMeasurementsFor( String operationName )
    {
        return runtimeMetrics.getMetric( operationName );
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
        return workloadStartTime;
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
}
