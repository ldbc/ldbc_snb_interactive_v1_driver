package com.ldbc.driver.measurements;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.measurements.exporters.MetricsExporter;
import com.ldbc.driver.measurements.formatters.MetricFormatter;
import com.ldbc.driver.measurements.metric.DiscreteMetric;
import com.ldbc.driver.measurements.metric.DiscreteMetricFactory;
import com.ldbc.driver.measurements.metric.HdrHistogramMetric;
import com.ldbc.driver.measurements.metric.HdrHistogramMetricFactory;
import com.ldbc.driver.measurements.metric.Metric;
import com.ldbc.driver.measurements.metric.MetricFactory;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

public class WorkloadMetricsManager
{
    private static Logger logger = Logger.getLogger( WorkloadMetricsManager.class );

    public static final String METRIC_RUNTIME = "Runtime";
    public static final String METRIC_START_TIME_DELAY = "Start Time Delay";
    public static final String METRIC_RESULT_CODE = "Result Code";

    private static final Duration MINUTES_10 = Duration.fromSeconds( 60 * 10 );

    public static final Duration DEFAULT_HIGHEST_EXPECTED_DURATION = MINUTES_10;
    public static final TimeUnit DEFAULT_DURATION_UNIT = TimeUnit.MICROSECONDS;

    private final MetricGroup<HdrHistogramMetric> runtimeMetrics;
    private final MetricGroup<HdrHistogramMetric> startTimeDelayMetrics;
    private final MetricGroup<DiscreteMetric> resultCodeMetrics;

    private final TimeUnit durationUnit;

    private Time startTime;
    private Time timeOfLastMeaurement = Time.now();
    private int measurementCount = 0;

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
        MetricFactory<HdrHistogramMetric> durationMetricFactory = new HdrHistogramMetricFactory(
                durationUnit.toString(), highestExpectedDuration.as( durationUnit ), 5 );
        MetricFactory<DiscreteMetric> resultCodeMetricFactory = new DiscreteMetricFactory( "Result" );
        runtimeMetrics = new MetricGroup<HdrHistogramMetric>( METRIC_RUNTIME, durationMetricFactory );
        startTimeDelayMetrics = new MetricGroup<HdrHistogramMetric>( METRIC_START_TIME_DELAY, durationMetricFactory );
        resultCodeMetrics = new MetricGroup<DiscreteMetric>( METRIC_RESULT_CODE, resultCodeMetricFactory );
        startTime = Time.now();
    }

    public void setStartTime( Time time )
    {
        startTime = time;
    }

    public void measure( OperationResult operationResult )
    {
        //
        // Number of operations measured
        //
        measurementCount++;

        //
        // Time last operation was measured
        //
        Time operationEndTime = operationResult.getActualStartTime().plus( operationResult.getRunTime() );
        if ( timeOfLastMeaurement.asNano() < operationEndTime.asNano() )
        {
            timeOfLastMeaurement = operationEndTime;
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
        Duration startTimeDelay = operationResult.getActualStartTime().greaterBy(
                operationResult.getScheduledStartTime() );
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

    public MetricGroup<HdrHistogramMetric> getRuntimes()
    {
        return runtimeMetrics;
    }

    public MetricGroup<HdrHistogramMetric> getStartTimeDelays()
    {
        return startTimeDelayMetrics;
    }

    public MetricGroup<DiscreteMetric> getResultCodes()
    {
        return resultCodeMetrics;
    }

    public Time getTimeOfFirstMeasurement()
    {
        return startTime;
    }

    public Time getTimeOfLastMeasurement()
    {
        return timeOfLastMeaurement;
    }

    public void export( MetricsExporter metricsExporter,
            MetricFormatter<HdrHistogramMetric> hdrHistogramMetricsFormatter,
            MetricFormatter<DiscreteMetric> discreteMetricsFormatter ) throws MetricsExporterException
    {
        metricsExporter.export( hdrHistogramMetricsFormatter, getRuntimes() );
        metricsExporter.export( hdrHistogramMetricsFormatter, getStartTimeDelays() );
        metricsExporter.export( discreteMetricsFormatter, getResultCodes() );
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

    public Integer getMeasurementCount()
    {
        return measurementCount;
    }

    public synchronized String getStatusString()
    {
        return statusString( Time.now() );
    }

    private String statusString( Time atTime )
    {
        DecimalFormat throughputFormat = new DecimalFormat( "#.00" );
        return String.format( "Status: Runtime (sec) [%s], Operations [%s], Throughput (op/sec) [%s]",
                getElapsedTime( atTime ).asSeconds(), measurementCount,
                throughputFormat.format( getThroughputAt( atTime ) ) );
    }

    private double getThroughputAt( Time atTime )
    {
        return (double) measurementCount / getElapsedTime( atTime ).asSeconds();
    }

    private Duration getElapsedTime( Time atTime )
    {
        return atTime.greaterBy( startTime );
    }
}
