package com.ldbc.driver.measurements.formatters;

import com.ldbc.driver.measurements.MetricGroup;
import com.ldbc.driver.measurements.metric.ContinuousMetric;

public class HdrHistogramMetricSimpleFormatter implements MetricFormatter<ContinuousMetric>
{
    private static final String DEFAULT_NAME = "<no name given>";
    private static final String DEFAULT_UNIT = "<no unit given>";

    @Override
    public String format( ContinuousMetric... metrics )
    {
        StringBuilder sb = new StringBuilder();
        for ( ContinuousMetric metric : metrics )
        {
            sb.append( formatOneMetric( "", metric ) );
        }
        return sb.toString();
    }

    @Override
    public String format( MetricGroup<ContinuousMetric>... metricGroups )
    {
        StringBuilder sb = new StringBuilder();
        for ( MetricGroup<ContinuousMetric> metricGroup : metricGroups )
        {
            sb.append( formatOneMetricGroup( metricGroup ) );
        }
        return sb.toString();
    }

    private String formatOneMetricGroup( MetricGroup<ContinuousMetric> metricGroup )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metricGroup.getName() ) ? DEFAULT_NAME : metricGroup.getName();
        sb.append( String.format( "%s\n", name ) );

        for ( ContinuousMetric metric : metricGroup.getMetrics() )
        {
            sb.append( formatOneMetric( "\t", metric ) );
        }
        return sb.toString();
    }

    private String formatOneMetric( String offset, ContinuousMetric metric )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metric.name() ) ? DEFAULT_NAME : metric.name();
        String unit = ( null == metric.unit() ) ? DEFAULT_UNIT : metric.unit();
        sb.append( offset ).append( String.format( "%s\n", name ) );
        sb.append( offset ).append( String.format( "\tUnits:\t\t\t%s\n", unit ) );
        sb.append( offset ).append( String.format( "\tCount:\t\t\t%s\n", metric.count() ) );
        sb.append( offset ).append( String.format( "\tMin:\t\t\t%s\n", metric.min() ) );
        sb.append( offset ).append( String.format( "\tMax:\t\t\t%s\n", metric.max() ) );
        sb.append( offset ).append( String.format( "\tMean:\t\t\t%s\n", metric.mean() ) );
        sb.append( offset ).append( String.format( "\t50th Percentile:\t%s\n", metric.percentile( 50 ) ) );
        sb.append( offset ).append( String.format( "\t90th Percentile:\t%s\n", metric.percentile( 90 ) ) );
        sb.append( offset ).append( String.format( "\t95th Percentile:\t%s\n", metric.percentile( 95 ) ) );
        sb.append( offset ).append( String.format( "\t99th Percentile:\t%s\n", metric.percentile( 99 ) ) );
        sb.append( offset ).append( String.format( "\t99.9th Percentile:\t%s\n", metric.percentile( 99.9 ) ) );
        return sb.toString();
    }
}
