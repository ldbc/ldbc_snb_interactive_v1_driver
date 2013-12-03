package com.ldbc.driver.measurements.formatters;

import com.ldbc.driver.measurements.MetricGroup;
import com.ldbc.driver.measurements.metric.HdrHistogramMetric;

public class HdrHistogramMetricSimpleFormatter implements MetricFormatter<HdrHistogramMetric>
{
    private static final String DEFAULT_NAME = "<no name given>";
    private static final String DEFAULT_UNIT = "<no unit given>";

    @Override
    public String format( HdrHistogramMetric... metrics )
    {
        StringBuilder sb = new StringBuilder();
        for ( HdrHistogramMetric metric : metrics )
        {
            sb.append( formatOneMetric( "", metric ) );
        }
        return sb.toString();
    }

    @Override
    public String format( MetricGroup<HdrHistogramMetric>... metricGroups )
    {
        StringBuilder sb = new StringBuilder();
        for ( MetricGroup<HdrHistogramMetric> metricGroup : metricGroups )
        {
            sb.append( formatOneMetricGroup( metricGroup ) );
        }
        return sb.toString();
    }

    private String formatOneMetricGroup( MetricGroup<HdrHistogramMetric> metricGroup )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metricGroup.getName() ) ? DEFAULT_NAME : metricGroup.getName();
        sb.append( String.format( "%s\n", name ) );

        for ( HdrHistogramMetric metric : metricGroup.getMetrics() )
        {
            sb.append( formatOneMetric( "\t", metric ) );
        }
        return sb.toString();
    }

    private String formatOneMetric( String offset, HdrHistogramMetric metric )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metric.getName() ) ? DEFAULT_NAME : metric.getName();
        String unit = ( null == metric.getUnit() ) ? DEFAULT_UNIT : metric.getUnit();
        sb.append( offset ).append( String.format( "%s\n", name ) );
        sb.append( offset ).append( String.format( "\tUnits:\t\t\t%s\n", unit ) );
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
