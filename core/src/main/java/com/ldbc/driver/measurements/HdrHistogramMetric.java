package com.ldbc.driver.measurements;

import org.HdrHistogram.Histogram;

import com.ldbc.driver.measurements.formatters.SimpleMetricsFormatter;
import com.ldbc.driver.util.Duration;

public class HdrHistogramMetric implements Metric
{
    private final Histogram histogram;
    private final String name;

    public HdrHistogramMetric( Duration highestExpectedValue )
    {
        this( null, highestExpectedValue );
    }

    public HdrHistogramMetric( String name, Duration highestExpectedValue )
    {
        histogram = new Histogram( highestExpectedValue.asNano(), 5 );
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void addMeasurement( long value )
    {
        histogram.recordValue( value );
    }

    @Override
    public double getMean()
    {
        return histogram.getHistogramData().getMean();
    }

    @Override
    public long getPercentile( double percentile )
    {
        return histogram.getHistogramData().getValueAtPercentile( percentile );
    }

    @Override
    public long getMin()
    {
        return histogram.getHistogramData().getMinValue();
    }

    @Override
    public long getMax()
    {
        return histogram.getHistogramData().getMaxValue();
    }

    @Override
    public long getCount()
    {
        return histogram.getHistogramData().getTotalCount();
    }

    @Override
    public String toPrettyString()
    {
        return new SimpleMetricsFormatter().format( this );
    }
}
