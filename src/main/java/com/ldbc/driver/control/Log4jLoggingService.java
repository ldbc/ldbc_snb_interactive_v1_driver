package com.ldbc.driver.control;

import com.ldbc.driver.runtime.metrics.SimpleDetailedWorkloadMetricsFormatter;
import com.ldbc.driver.runtime.metrics.SimpleSummaryWorkloadMetricsFormatter;
import com.ldbc.driver.runtime.metrics.WorkloadMetricsFormatter;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.runtime.metrics.WorkloadStatusSnapshot;
import com.ldbc.driver.temporal.TemporalUtil;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

public class Log4jLoggingService implements LoggingService
{
    private static final DecimalFormat OPERATION_COUNT_FORMATTER = new DecimalFormat( "###,###,###,###" );
    private static final DecimalFormat THROUGHPUT_FORMATTER = new DecimalFormat( "###,###,###,##0.00" );

    private final Logger logger;
    private final TemporalUtil temporalUtil;
    private final boolean detailedStatus;
    private final WorkloadMetricsFormatter summaryWorkloadMetricsFormatter;
    private final WorkloadMetricsFormatter detailedWorkloadMetricsFormatter;

    public Log4jLoggingService( String source, TemporalUtil temporalUtil, boolean detailedStatus )
    {
        this.logger = Logger.getLogger( source );
        this.temporalUtil = temporalUtil;
        this.detailedStatus = detailedStatus;
        this.summaryWorkloadMetricsFormatter = new SimpleSummaryWorkloadMetricsFormatter();
        this.detailedWorkloadMetricsFormatter = new SimpleDetailedWorkloadMetricsFormatter();
    }

    @Override
    public void info( String message )
    {
        logger.info( message );
    }

    @Override
    public void status(
            WorkloadStatusSnapshot status,
            RecentThroughputAndDuration recentThroughputAndDuration,
            long globalCompletionTimeAsMilli )
    {
        String statusString;
        statusString = (detailedStatus) ?
                       formatWithGct(
                               status.operationCount(),
                               status.runDurationAsMilli(),
                               status.durationSinceLastMeasurementAsMilli(),
                               status.throughput(),
                               recentThroughputAndDuration.throughput(),
                               recentThroughputAndDuration.duration(),
                               globalCompletionTimeAsMilli ) :
                       formatWithoutGct(
                               status.operationCount(),
                               status.runDurationAsMilli(),
                               status.durationSinceLastMeasurementAsMilli(),
                               status.throughput(),
                               recentThroughputAndDuration.throughput(),
                               recentThroughputAndDuration.duration() );
        logger.info( statusString );
    }

    @Override
    public void summaryResult( WorkloadResultsSnapshot workloadResultsSnapshot )
    {
        logger.info( "\n" + summaryWorkloadMetricsFormatter.format( workloadResultsSnapshot ) );
    }

    @Override
    public void detailedResult( WorkloadResultsSnapshot workloadResultsSnapshot )
    {
        logger.info( "\n" + detailedWorkloadMetricsFormatter.format( workloadResultsSnapshot ) );
    }

    private String formatWithoutGct( long operationCount, long runDurationAsMilli,
            long durationSinceLastMeasurementAsMilli, double throughput, double recentThroughput,
            long recentDurationAsMilli )
    {
        return format( operationCount, runDurationAsMilli, durationSinceLastMeasurementAsMilli, throughput,
                recentThroughput, recentDurationAsMilli, null ).toString();
    }

    private String formatWithGct( long operationCount, long runDurationAsMilli,
            long durationSinceLastMeasurementAsMilli, double throughput, double recentThroughput,
            long recentDurationAsMilli, long gctAsMilli )
    {
        return format( operationCount, runDurationAsMilli, durationSinceLastMeasurementAsMilli, throughput,
                recentThroughput, recentDurationAsMilli, gctAsMilli ).toString();
    }

    private StringBuffer format( long operationCount, long runDurationAsMilli, long durationSinceLastMeasurementAsMilli,
            double throughput, double recentThroughput, long recentDurationAsMilli, Long gctAsMilli )
    {
        StringBuffer sb = new StringBuffer();
        sb.append( "Runtime [" )
                .append( (-1 == runDurationAsMilli) ? "--" : temporalUtil.milliDurationToString( runDurationAsMilli ) )
                .append( "], " );
        sb.append( "Operations [" ).append( OPERATION_COUNT_FORMATTER.format( operationCount ) ).append( "], " );
        sb.append( "Last [" ).append( (-1 == durationSinceLastMeasurementAsMilli) ? "--" : temporalUtil
                .milliDurationToString( durationSinceLastMeasurementAsMilli ) ).append( "], " );
        sb.append( "Throughput" );
        sb.append( " (Total) [" ).append( THROUGHPUT_FORMATTER.format( throughput ) ).append( "]" );
        sb.append( " (Last " ).append( TimeUnit.MILLISECONDS.toSeconds( recentDurationAsMilli ) ).append( "s) [" )
                .append( THROUGHPUT_FORMATTER.format( recentThroughput ) ).append( "]" );
        if ( null != gctAsMilli )
        {
            sb.append(
                    ", GCT: " + ((-1 == gctAsMilli) ? "--" : temporalUtil.milliTimeToDateTimeString( gctAsMilli )) );
        }
        return sb;
    }
}
