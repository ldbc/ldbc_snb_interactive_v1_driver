package com.ldbc.driver.measurements.metric;

import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import com.ldbc.driver.measurements.MetricException;

public class HdrHistogramMetric implements Metric
{
    private static Logger logger = Logger.getLogger( HdrHistogramMetric.class );

    private final Histogram histogram;
    private final String name;
    private final String unit;

    public HdrHistogramMetric( long highestExpectedValue, int numberOfSignificantDigits )
    {
        this( null, null, highestExpectedValue, numberOfSignificantDigits );
    }

    public HdrHistogramMetric( String name, String unit, long highestExpectedValue, int numberOfSignificantDigits )
    {
        histogram = new Histogram( highestExpectedValue, numberOfSignificantDigits );
        this.name = name;
        this.unit = unit;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getUnit()
    {
        return unit;
    }

    @Override
    public void addMeasurement( long value )
    {
        try
        {
            histogram.recordValue( value );
        }
        catch ( ArrayIndexOutOfBoundsException e )
        {
            String errMsg = String.format( "Error encountered adding measurement [%s]", value );
            logger.error( errMsg, e );
            throw new MetricException( errMsg, e.getCause() );
        }
    }

    @Override
    public long getCount()
    {
        return histogram.getHistogramData().getTotalCount();
    }

    public long getCountAt( long value )
    {
        return histogram.getHistogramData().getCountAtValue( value );
    }

    public double getMean()
    {
        return histogram.getHistogramData().getMean();
    }

    public long getMin()
    {
        return histogram.getHistogramData().getMinValue();
    }

    public long getMax()
    {
        return histogram.getHistogramData().getMaxValue();
    }

    public long getPercentile( double percentile )
    {
        return histogram.getHistogramData().getValueAtPercentile( percentile );
    }
}
