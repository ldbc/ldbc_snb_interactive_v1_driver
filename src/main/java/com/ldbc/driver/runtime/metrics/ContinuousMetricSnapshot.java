package com.ldbc.driver.runtime.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ContinuousMetricSnapshot
{
    @JsonProperty( value = "name" )
    private String name;
    @JsonProperty( value = "unit" )
    private TimeUnit unit;
    @JsonProperty( value = "count" )
    private long count;
    @JsonProperty( value = "mean" )
    private double mean;
    @JsonProperty( value = "min" )
    private long min;
    @JsonProperty( value = "max" )
    private long max;
    @JsonProperty( value = "25th_percentile" )
    private long percentile25;
    @JsonProperty( value = "50th_percentile" )
    private long percentile50;
    @JsonProperty( value = "75th_percentile" )
    private long percentile75;
    @JsonProperty( value = "90th_percentile" )
    private long percentile90;
    @JsonProperty( value = "95th_percentile" )
    private long percentile95;
    @JsonProperty( value = "99th_percentile" )
    private long percentile99;
    @JsonProperty( value = "99.9th_percentile" )
    private long percentile99_9;
    @JsonProperty( value = "std_dev" )
    private double stdDev;

    private ContinuousMetricSnapshot()
    {
    }

    ContinuousMetricSnapshot( String name,
            TimeUnit unit,
            long count,
            double mean,
            long min,
            long max,
            long percentile25,
            long percentile50,
            long percentile75,
            long percentile90,
            long percentile95,
            long percentile99,
            long percentile99_9,
            double stdDev )
    {
        this.name = name;
        this.unit = unit;
        this.count = count;
        this.mean = mean;
        this.min = min;
        this.max = max;
        this.percentile25 = percentile25;
        this.percentile50 = percentile50;
        this.percentile75 = percentile75;
        this.percentile90 = percentile90;
        this.percentile95 = percentile95;
        this.percentile99 = percentile99;
        this.percentile99_9 = percentile99_9;
        this.stdDev = stdDev;
    }

    public String name()
    {
        return name;
    }

    public TimeUnit unit()
    {
        return unit;
    }

    public long count()
    {
        return count;
    }

    public double mean()
    {
        return mean;
    }

    public long min()
    {
        return min;
    }

    public long max()
    {
        return max;
    }

    public long percentile25()
    {
        return percentile25;
    }

    public long percentile50()
    {
        return percentile50;
    }

    public long percentile75()
    {
        return percentile75;
    }

    public long percentile90()
    {
        return percentile90;
    }

    public long percentile95()
    {
        return percentile95;
    }

    public long percentile99()
    {
        return percentile99;
    }

    public long percentile99_9()
    {
        return percentile99_9;
    }

    public double stdDev()
    {
        return stdDev;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        ContinuousMetricSnapshot that = (ContinuousMetricSnapshot) o;
        return count == that.count &&
               Double.compare( that.mean, mean ) == 0 &&
               min == that.min &&
               max == that.max &&
               percentile25 == that.percentile25 &&
               percentile50 == that.percentile50 &&
               percentile75 == that.percentile75 &&
               percentile90 == that.percentile90 &&
               percentile95 == that.percentile95 &&
               percentile99 == that.percentile99 &&
               percentile99_9 == that.percentile99_9 &&
               Double.compare( that.stdDev, stdDev ) == 0 &&
               Objects.equals( name, that.name ) &&
               unit == that.unit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name, unit, count, mean, min, max, percentile25, percentile50, percentile75, percentile90,
                percentile95, percentile99, percentile99_9, stdDev );
    }
}
