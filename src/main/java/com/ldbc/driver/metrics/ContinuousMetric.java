package com.ldbc.driver.metrics;

import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonProperty;


public class ContinuousMetric
{
    private static Logger logger = Logger.getLogger( ContinuousMetric.class );

    private final Histogram histogram;
    private final String name;
    private final String unit;

    public ContinuousMetric( long highestExpectedValue, int numberOfSignificantDigits )
    {
        this( null, null, highestExpectedValue, numberOfSignificantDigits );
    }

    public ContinuousMetric( String name, String unit, long highestExpectedValue, int numberOfSignificantDigits )
    {
        histogram = new Histogram( highestExpectedValue, numberOfSignificantDigits );
        this.name = name;
        this.unit = unit;
    }

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

    @JsonProperty( value = "name" )
    public String name()
    {
        return name;
    }

    @JsonProperty( value = "unit" )
    public String unit()
    {
        return unit;
    }

    @JsonProperty( value = "count" )
    public long count()
    {
        return histogram.getHistogramData().getTotalCount();
    }

    @JsonProperty( value = "mean" )
    public double mean()
    {
        return histogram.getHistogramData().getMean();
    }

    @JsonProperty( value = "min" )
    public long min()
    {
        return histogram.getHistogramData().getMinValue();
    }

    @JsonProperty( value = "max" )
    public long max()
    {
        return histogram.getHistogramData().getMaxValue();
    }

    public long percentile( double percentile )
    {
        return histogram.getHistogramData().getValueAtPercentile( percentile );
    }

    @JsonProperty( value = "50th_percentile" )
    public long percentile50()
    {
        return histogram.getHistogramData().getValueAtPercentile( 50 );
    }

    @JsonProperty( value = "90th_percentile" )
    public long percentile90()
    {
        return histogram.getHistogramData().getValueAtPercentile( 90 );
    }

    @JsonProperty( value = "95th_percentile" )
    public long percentile95()
    {
        return histogram.getHistogramData().getValueAtPercentile( 95 );
    }

    @JsonProperty( value = "99th_percentile" )
    public long percentile99()
    {
        return histogram.getHistogramData().getValueAtPercentile( 99 );
    }
}
