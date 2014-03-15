package com.ldbc.driver.runtime.metrics_NEW.formatters;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.ldbc.driver.runtime.metrics_NEW.OperationMetrics;
import com.ldbc.driver.runtime.metrics_NEW.OperationMetrics.OperationMetricsNameComparator;

public class SimpleOperationMetricsFormatter implements OperationMetricsFormatter
{
    private static final String DEFAULT_NAME = "<no name given>";
    private static final String DEFAULT_UNIT = "<no unit given>";

    public String format( Iterable<OperationMetrics> metrics )
    {
        List<OperationMetrics> sortedMetrics = Lists.newArrayList( metrics );
        Collections.sort( sortedMetrics, new OperationMetricsNameComparator() );

        StringBuilder sb = new StringBuilder();
        sb.append( "Runtime\n" );
        for ( OperationMetrics metric : sortedMetrics )
        {
            sb.append( formatOneMetricRuntime( "\t", metric ) );
        }
        sb.append( "Start Time Delay\n" );
        for ( OperationMetrics metric : sortedMetrics )
        {
            sb.append( formatOneMetricStartTimeDelay( "\t", metric ) );
        }
        sb.append( "Result\n" );
        for ( OperationMetrics metric : sortedMetrics )
        {
            sb.append( formatOneMetricResult( "\t", metric ) );
        }
        return sb.toString();
    }

    private String formatOneMetricRuntime( String offset, OperationMetrics metric )
    {
        String name = ( null == metric.name() ) ? DEFAULT_NAME : metric.name();
        String unit = ( null == metric.durationUnit() ) ? DEFAULT_UNIT : metric.durationUnit().toString();
        StringBuilder sb = new StringBuilder();
        sb.append( offset ).append( String.format( "%s\n", name ) );
        sb.append( offset ).append( String.format( "\tUnits:\t\t\t%s\n", unit ) );
        sb.append( offset ).append( String.format( "\tCount:\t\t\t%s\n", metric.runTimeMetric().count() ) );
        sb.append( offset ).append( String.format( "\tMin:\t\t\t%s\n", metric.runTimeMetric().min() ) );
        sb.append( offset ).append( String.format( "\tMax:\t\t\t%s\n", metric.runTimeMetric().max() ) );
        sb.append( offset ).append( String.format( "\tMean:\t\t\t%s\n", metric.runTimeMetric().mean() ) );
        sb.append( offset ).append( String.format( "\t50th Percentile:\t%s\n", metric.runTimeMetric().percentile50() ) );
        sb.append( offset ).append( String.format( "\t90th Percentile:\t%s\n", metric.runTimeMetric().percentile90() ) );
        sb.append( offset ).append( String.format( "\t95th Percentile:\t%s\n", metric.runTimeMetric().percentile95() ) );
        sb.append( offset ).append( String.format( "\t99th Percentile:\t%s\n", metric.runTimeMetric().percentile99() ) );
        return sb.toString();
    }

    private String formatOneMetricStartTimeDelay( String offset, OperationMetrics metric )
    {
        String name = ( null == metric.name() ) ? DEFAULT_NAME : metric.name();
        String unit = ( null == metric.durationUnit() ) ? DEFAULT_UNIT : metric.durationUnit().toString();
        StringBuilder sb = new StringBuilder();
        sb.append( offset ).append( String.format( "%s\n", name ) );
        sb.append( offset ).append( String.format( "\tUnits:\t\t\t%s\n", unit ) );
        sb.append( offset ).append( String.format( "\tCount:\t\t\t%s\n", metric.startTimeDelayMetric().count() ) );
        sb.append( offset ).append( String.format( "\tMin:\t\t\t%s\n", metric.startTimeDelayMetric().min() ) );
        sb.append( offset ).append( String.format( "\tMax:\t\t\t%s\n", metric.startTimeDelayMetric().max() ) );
        sb.append( offset ).append( String.format( "\tMean:\t\t\t%s\n", metric.startTimeDelayMetric().mean() ) );
        sb.append( offset ).append(
                String.format( "\t50th Percentile:\t%s\n", metric.startTimeDelayMetric().percentile50() ) );
        sb.append( offset ).append(
                String.format( "\t90th Percentile:\t%s\n", metric.startTimeDelayMetric().percentile90() ) );
        sb.append( offset ).append(
                String.format( "\t95th Percentile:\t%s\n", metric.startTimeDelayMetric().percentile95() ) );
        sb.append( offset ).append(
                String.format( "\t99th Percentile:\t%s\n", metric.startTimeDelayMetric().percentile99() ) );
        return sb.toString();
    }

    private String formatOneMetricResult( String offset, OperationMetrics metric )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metric.name() ) ? DEFAULT_NAME : metric.name();
        String unit = ( null == metric.durationUnit() ) ? DEFAULT_UNIT : metric.durationUnit().toString();
        sb.append( offset ).append( String.format( "%s\n", name ) );
        sb.append( offset ).append( String.format( "\tUnits:\t\t\t%s\n", unit ) );
        sb.append( offset ).append( String.format( "\tCount:\t\t\t%s\n", metric.resultCodeMetric().count() ) );
        sb.append( offset ).append( String.format( "\tValues:\n" ) );
        for ( Entry<Long, Long> measurement : metric.resultCodeMetric().getAllValues().entrySet() )
        {
            sb.append( offset ).append( String.format( "\t\t%s:\t\t%s\n", measurement.getKey(), measurement.getValue() ) );
        }
        return sb.toString();
    }
}
