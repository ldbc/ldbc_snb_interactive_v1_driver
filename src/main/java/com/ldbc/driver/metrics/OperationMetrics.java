package com.ldbc.driver.metrics;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.annotate.JsonProperty;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.util.temporal.Duration;

public class OperationMetrics
{
    private static final String METRIC_RUNTIME = "Runtime";
    private static final String METRIC_START_TIME_DELAY = "Start Time Delay";
    private static final String METRIC_RESULT_CODE = "Result Code";

    private static final int NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS = 5;

    private final ContinuousMetric runTimeMetric;
    private final ContinuousMetric startTimeDelayMetric;
    private final DiscreteMetric resultCodeMetric;
    private final String name;
    private final TimeUnit durationUnit;

    public OperationMetrics( String name, TimeUnit durationUnit, Duration highestExpectedDuration )
    {
        this.name = name;
        this.durationUnit = durationUnit;

        this.runTimeMetric = new ContinuousMetric( METRIC_RUNTIME, durationUnit.toString(),
                highestExpectedDuration.as( durationUnit ), NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS );

        this.startTimeDelayMetric = new ContinuousMetric( METRIC_START_TIME_DELAY, durationUnit.toString(),
                highestExpectedDuration.as( durationUnit ), NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS );

        this.resultCodeMetric = new DiscreteMetric( METRIC_RESULT_CODE, "Result Code" );
    }

    public void measure( OperationResult operationResult )
    {
        //
        // Measure operation runtime
        //
        long runtimeInAppropriateUnit = operationResult.runDuration().as( durationUnit );
        try
        {
            runTimeMetric.addMeasurement( runtimeInAppropriateUnit );
        }
        catch ( MetricException e )
        {
            String errMsg = String.format( "Error encountered adding runtime [%s %s] to [%s]",
                    runtimeInAppropriateUnit, durationUnit.toString(), name );
            throw new MetricException( errMsg, e.getCause() );
        }

        //
        // Measure driver performance - how close is it to target throughput
        //
        Duration startTimeDelay = operationResult.actualStartTime().greaterBy( operationResult.scheduledStartTime() );
        long startTimeDelayInAppropriateUnit = startTimeDelay.as( durationUnit );
        try
        {
            startTimeDelayMetric.addMeasurement( startTimeDelayInAppropriateUnit );
        }
        catch ( MetricException e )
        {
            String errMsg = String.format( "Error encountered adding start time delay measurement [%s %s] to [%s]",
                    startTimeDelayInAppropriateUnit, durationUnit.toString(), name );
            throw new MetricException( errMsg, e.getCause() );
        }

        //
        // Measure result code
        //
        int operationResultCode = operationResult.resultCode();
        try
        {
            resultCodeMetric.addMeasurement( operationResultCode );
        }
        catch ( MetricException e )
        {
            String errMsg = String.format( "Error encountered adding result code measurement [%s] to [%s]",
                    operationResultCode, name );
            throw new MetricException( errMsg, e.getCause() );
        }
    }

    @JsonProperty( "name" )
    public String name()
    {
        return name;
    }

    @JsonProperty( "unit" )
    public TimeUnit durationUnit()
    {
        return durationUnit;
    }

    /*
     * Metrics
     */

    @JsonProperty( "run_time" )
    public ContinuousMetric runTimeMetric()
    {
        return runTimeMetric;
    }

    @JsonProperty( "start_time_delay" )
    public ContinuousMetric startTimeDelayMetric()
    {
        return startTimeDelayMetric;
    }

    @JsonProperty( "result_code" )
    public DiscreteMetric resultCodeMetric()
    {
        return resultCodeMetric;
    }

    public static class OperationMetricsNameComparator implements Comparator<OperationMetrics>
    {
        private static final String EMPTY_STRING = "";

        @Override
        public int compare( OperationMetrics metrics1, OperationMetrics metrics2 )
        {
            String metrics1Name = ( metrics1.name() == null ) ? EMPTY_STRING : metrics1.name();
            String metrics2Name = ( metrics2.name() == null ) ? EMPTY_STRING : metrics2.name();
            return metrics1Name.compareTo( metrics2Name );
        }
    }
}
