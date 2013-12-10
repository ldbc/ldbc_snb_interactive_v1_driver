package com.ldbc.driver.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.annotate.JsonProperty;

import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Histogram;

public class DiscreteMetric
{
    private final Histogram<Long, Integer> measurements;
    private final String name;
    private final String unit;
    private long measurementMin = Long.MAX_VALUE;
    private long measurementMax = Long.MIN_VALUE;

    public DiscreteMetric( String name, String unit )
    {
        this.measurements = new Histogram<Long, Integer>( 0 );
        this.name = name;
        this.unit = unit;
    }

    public void addMeasurement( long value )
    {
        measurements.incOrCreateBucket( DiscreteBucket.create( value ), 1 );
        measurementMin = ( value < measurementMin ) ? value : measurementMin;
        measurementMax = ( value > measurementMax ) ? value : measurementMax;
    }

    @JsonProperty( "name" )
    public String name()
    {
        return name;
    }

    @JsonProperty( "unit" )
    public String unit()
    {
        return unit;
    }

    @JsonProperty( "count" )
    public long count()
    {
        return measurements.sumOfAllBucketValues();
    }

    @JsonProperty( "all_values" )
    public Map<Long, Long> getAllValues()
    {
        Map<Long, Long> allValuesMap = new HashMap<Long, Long>();
        for ( Entry<Bucket<Long>, Integer> entry : measurements.getAllBuckets() )
        {
            long resultCode = ( (DiscreteBucket<Long>) entry.getKey() ).getId();
            long resultCodeCount = entry.getValue();
            allValuesMap.put( resultCode, resultCodeCount );
        }
        return allValuesMap;
    }
}
