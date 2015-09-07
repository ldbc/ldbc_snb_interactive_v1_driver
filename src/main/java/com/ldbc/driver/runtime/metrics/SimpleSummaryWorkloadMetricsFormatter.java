package com.ldbc.driver.runtime.metrics;

import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.TemporalUtil;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SimpleSummaryWorkloadMetricsFormatter implements WorkloadMetricsFormatter
{
    private static final String DEFAULT_NAME = "<no name given>";
    private static final String DEFAULT_UNIT = "<no unit given>";
    private static final String OFFSET = "    ";
    private static final DecimalFormat INTEGER_FORMATTER = new DecimalFormat( "###,###,###,###" );
    private static final DecimalFormat FLOAT_FORMATTER = new DecimalFormat( "###,###,###,##0.00" );
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();

    public String format( WorkloadResultsSnapshot workloadResultsSnapshot )
    {
        List<OperationMetricsSnapshot> sortedMetrics = Lists.newArrayList( workloadResultsSnapshot.allMetrics() );
        Collections.sort( sortedMetrics, new OperationTypeMetricsManager.OperationMetricsNameComparator() );

        int padRightDistance = 40;
        StringBuilder sb = new StringBuilder();
        sb.append( "------------------------------------------------------------------------------\n" );
        sb.append( String.format( "%1$-" + padRightDistance + "s", "Operation Count:" ) ).append(
                INTEGER_FORMATTER.format( workloadResultsSnapshot.totalOperationCount() ) ).append( "\n" );
        sb.append( String.format( "%1$-" + padRightDistance + "s", "Duration:" ) ).append(
                TEMPORAL_UTIL.nanoDurationToString( workloadResultsSnapshot.totalRunDurationAsNano() ) ).append( "\n" );
        double opsPerNs = (workloadResultsSnapshot.totalOperationCount() /
                           (double) workloadResultsSnapshot.totalRunDurationAsNano());
        double opsPerS = opsPerNs * TimeUnit.SECONDS.toNanos( 1 );
        sb.append( String.format( "%1$-" + padRightDistance + "s", "Throughput:" ) ).append(
                FLOAT_FORMATTER.format( opsPerS ) ).append( " (op/s)\n" );
        sb.append( "------------------------------------------------------------------------------\n" );
        int namePadRightDistance = 0;
        int countPadRightDistance = 0;
        for ( OperationMetricsSnapshot metric : sortedMetrics )
        {
            namePadRightDistance = Math.max( namePadRightDistance, metric.name().length() );
            countPadRightDistance = Math.max( countPadRightDistance, Long.toString( metric.count() ).length() );
        }
        for ( OperationMetricsSnapshot metric : sortedMetrics )
        {
            sb.append( formatOneMetricRuntime( OFFSET, metric, namePadRightDistance + 2, countPadRightDistance + 2 ) );
        }
        sb.append( "------------------------------------------------------------------------------\n" );
        return sb.toString();
    }

    private String formatOneMetricRuntime( String offset, OperationMetricsSnapshot metric, int namePadRightDistance,
            int countPadRightDistance )
    {
        String name = (null == metric.name()) ? DEFAULT_NAME : metric.name();
        String unit = (null == metric.durationUnit()) ? DEFAULT_UNIT
                                                      : TEMPORAL_UTIL.abbreviatedTimeUnit( metric.durationUnit() );
        StringBuilder sb = new StringBuilder();
        sb.append( offset );
        sb.append( String.format( "%1$-" + namePadRightDistance + "s", name ) );
        sb.append( "Count: " ).append( String.format( "%1$-" + countPadRightDistance + "s",
                INTEGER_FORMATTER.format( metric.runTimeMetric().count() ) ) ).append( " " );
        sb.append( "Mean: " ).append( FLOAT_FORMATTER.format( metric.runTimeMetric().mean() ) ).append( " " )
                .append( unit ).append( "\n" );
        return sb.toString();
    }
}
