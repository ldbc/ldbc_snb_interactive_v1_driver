package com.ldbc.driver.measurements.formatters;

import com.ldbc.driver.measurements.Metric;
import com.ldbc.driver.measurements.MetricGroup;

public class SimpleMetricsFormatter implements MetricsFormatter
{
    private static final String DEFAULT_NAME = "<no name given>";

    @Override
    public String format( Metric... metrics )
    {
        StringBuilder sb = new StringBuilder();
        for ( Metric metric : metrics )
        {
            sb.append( formatOneMetric( "", metric ) );
        }
        return sb.toString();
    }

    @Override
    public String format( MetricGroup... metricGroups )
    {
        StringBuilder sb = new StringBuilder();
        for ( MetricGroup metricGroup : metricGroups )
        {
            sb.append( formatOneMetricGroup( metricGroup ) );
        }
        return sb.toString();
    }

    private String formatOneMetricGroup( MetricGroup metricGroup )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metricGroup.getName() ) ? DEFAULT_NAME : metricGroup.getName();
        sb.append( String.format( "%s\n", name ) );

        for ( Metric metric : metricGroup.getMetrics() )
        {
            sb.append( formatOneMetric( "\t", metric ) );
        }
        return sb.toString();
    }

    private String formatOneMetric( String offset, Metric metric )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metric.getName() ) ? DEFAULT_NAME : metric.getName();
        sb.append( offset ).append( String.format( "%s\n", name ) );
        sb.append( offset ).append( String.format( "\tCount:\t\t\t%s\n", metric.getCount() ) );
        sb.append( offset ).append( String.format( "\tMin:\t\t\t%s\n", metric.getMin() ) );
        sb.append( offset ).append( String.format( "\tMax:\t\t\t%s\n", metric.getMax() ) );
        sb.append( offset ).append( String.format( "\tMean:\t\t\t%s\n", metric.getMean() ) );
        sb.append( offset ).append( String.format( "\t50th Percentile:\t%s\n", metric.getPercentile( 50 ) ) );
        sb.append( offset ).append( String.format( "\t90th Percentile:\t%s\n", metric.getPercentile( 90 ) ) );
        sb.append( offset ).append( String.format( "\t95th Percentile:\t%s\n", metric.getPercentile( 95 ) ) );
        sb.append( offset ).append( String.format( "\t99th Percentile:\t%s\n", metric.getPercentile( 99 ) ) );
        sb.append( offset ).append( String.format( "\t99.9th Percentile:\t%s\n", metric.getPercentile( 99.9 ) ) );
        return sb.toString();
    }
}
