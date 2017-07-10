package com.ldbc.driver.control;

import com.ldbc.driver.runtime.metrics.SimpleDetailedWorkloadMetricsFormatter;
import com.ldbc.driver.runtime.metrics.SimpleSummaryWorkloadMetricsFormatter;
import com.ldbc.driver.runtime.metrics.WorkloadMetricsFormatter;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.runtime.metrics.WorkloadStatusSnapshot;
import com.ldbc.driver.temporal.TemporalUtil;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Log4jLoggingService implements LoggingService
{
    private static final DecimalFormat OPERATION_COUNT_FORMATTER = new DecimalFormat( "###,###,###,###" );
    private static final DecimalFormat THROUGHPUT_FORMATTER = new DecimalFormat( "###,###,###,##0.00" );
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat( "YYYY/MM/dd HH:mm:ss Z" );

    private final Logger logger;
    private final TemporalUtil temporalUtil;
    private final boolean detailedStatus;
    private final WorkloadMetricsFormatter summaryWorkloadMetricsFormatter;
    private final WorkloadMetricsFormatter detailedWorkloadMetricsFormatter;

    Log4jLoggingService( String source, TemporalUtil temporalUtil, boolean detailedStatus )
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
            long completionTimeAsMilli )
    {
        String statusString;
        statusString = (detailedStatus) ?
                       formatWithCt(
                               status.operationCount(),
                               status.runDurationAsMilli(),
                               status.durationSinceLastMeasurementAsMilli(),
                               status.throughput(),
                               recentThroughputAndDuration.throughput(),
                               recentThroughputAndDuration.duration(),
                               completionTimeAsMilli ) :
                       formatWithoutCt(
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

    private String formatWithoutCt(
            long operationCount,
            long runDurationAsMilli,
            long durationSinceLastMeasurementAsMilli,
            double throughput,
            double recentThroughput,
            long recentDurationAsMilli )
    {
        return format(
                operationCount,
                runDurationAsMilli,
                durationSinceLastMeasurementAsMilli,
                throughput,
                recentThroughput,
                recentDurationAsMilli,
                null ).toString();
    }

    private String formatWithCt(
            long operationCount,
            long runDurationAsMilli,
            long durationSinceLastMeasurementAsMilli,
            double throughput,
            double recentThroughput,
            long recentDurationAsMilli,
            long ctAsMilli )
    {
        return format(
                operationCount,
                runDurationAsMilli,
                durationSinceLastMeasurementAsMilli,
                throughput,
                recentThroughput,
                recentDurationAsMilli,
                ctAsMilli ).toString();
    }

    private StringBuffer format(
            long operationCount,
            long runDurationAsMilli,
            long durationSinceLastMeasurementAsMilli,
            double throughput,
            double recentThroughput,
            long recentDurationAsMilli,
            Long ctAsMilli )
    {
        StringBuffer sb = new StringBuffer()
                .append( DATE_FORMAT.format( new Date() ) ).append( " " )
                .append( "Runtime [" )
                .append( (-1 == runDurationAsMilli)
                         ? "--"
                         : temporalUtil.milliDurationToString( runDurationAsMilli ) ).append( "], " )
                .append( "Operations [" ).append( OPERATION_COUNT_FORMATTER.format( operationCount ) ).append( "], " )
                .append( "Last [" )
                .append( (-1 == durationSinceLastMeasurementAsMilli)
                         ? "--"
                         : temporalUtil.milliDurationToString( durationSinceLastMeasurementAsMilli ) ).append( "], " )
                .append( "Throughput" )
                .append( " (Total) [" ).append( THROUGHPUT_FORMATTER.format( throughput ) ).append( "]" )
                .append( " (Last " ).append( TimeUnit.MILLISECONDS.toSeconds( recentDurationAsMilli ) )
                .append( "s) [" ).append( THROUGHPUT_FORMATTER.format( recentThroughput ) ).append( "]" );
        if ( null != ctAsMilli )
        {
            sb.append( ", CT: " + ((-1 == ctAsMilli) ? "--" : temporalUtil.milliTimeToDateTimeString( ctAsMilli )) );
        }
        return sb;
    }
}
