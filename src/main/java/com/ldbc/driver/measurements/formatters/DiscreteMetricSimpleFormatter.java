package com.ldbc.driver.measurements.formatters;

import java.util.Iterator;

import com.ldbc.driver.measurements.MetricGroup;
import com.ldbc.driver.measurements.metric.DiscreteMetric;
import com.ldbc.driver.util.Tuple.Tuple2;

public class DiscreteMetricSimpleFormatter implements MetricFormatter<DiscreteMetric>
{
    private static final String DEFAULT_NAME = "<no name given>";
    private static final String DEFAULT_UNIT = "<no unit given>";

    @Override
    public String format( DiscreteMetric... metrics )
    {
        StringBuilder sb = new StringBuilder();
        for ( DiscreteMetric metric : metrics )
        {
            sb.append( formatOneMetric( "", metric ) );
        }
        return sb.toString();
    }

    @Override
    public String format( MetricGroup<DiscreteMetric>... metricGroups )
    {
        StringBuilder sb = new StringBuilder();
        for ( MetricGroup<DiscreteMetric> metricGroup : metricGroups )
        {
            sb.append( formatOneMetricGroup( metricGroup ) );
        }
        return sb.toString();
    }

    private String formatOneMetricGroup( MetricGroup<DiscreteMetric> metricGroup )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metricGroup.getName() ) ? DEFAULT_NAME : metricGroup.getName();
        sb.append( String.format( "%s\n", name ) );

        for ( DiscreteMetric metric : metricGroup.getMetrics() )
        {
            sb.append( formatOneMetric( "\t", metric ) );
        }
        return sb.toString();
    }

    private String formatOneMetric( String offset, DiscreteMetric metric )
    {
        StringBuilder sb = new StringBuilder();
        String name = ( null == metric.getName() ) ? DEFAULT_NAME : metric.getName();
        String unit = ( null == metric.getUnit() ) ? DEFAULT_UNIT : metric.getUnit();
        sb.append( offset ).append( String.format( "%s\n", name ) );
        sb.append( offset ).append( String.format( "\tUnits:\t\t\t%s\n", unit ) );
        sb.append( offset ).append( String.format( "\tCount:\t\t\t%s\n", metric.getCount() ) );
        sb.append( offset ).append( String.format( "\tValues:\n" ) );
        Iterator<Tuple2<Long, Integer>> measurementValues = metric.getAllValues();
        while ( measurementValues.hasNext() )
        {
            Tuple2<Long, Integer> measurement = measurementValues.next();
            sb.append( offset ).append( String.format( "\t\t%s:\t\t%s\n", measurement._1(), measurement._2() ) );
        }

        return sb.toString();
    }
}
