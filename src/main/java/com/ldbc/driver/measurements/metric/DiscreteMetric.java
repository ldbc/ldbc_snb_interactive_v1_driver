package com.ldbc.driver.measurements.metric;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.codehaus.jackson.annotate.JsonProperty;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Histogram;

//TODO remove Metric abstraction?
public class DiscreteMetric implements Metric
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

    @Override
    public void addMeasurement( long value )
    {
        measurements.incOrCreateBucket( DiscreteBucket.create( value ), 1 );
        measurementMin = ( value < measurementMin ) ? value : measurementMin;
        measurementMax = ( value > measurementMax ) ? value : measurementMax;
    }

    @JsonProperty( value = "name" )
    @Override
    public String name()
    {
        return name;
    }

    @JsonProperty( value = "unit" )
    @Override
    public String unit()
    {
        return unit;
    }

    @JsonProperty( value = "count" )
    @Override
    public long count()
    {
        return measurements.sumOfAllBucketValues();
    }

    @JsonProperty( value = "all_values" )
    public List<Long[]> getAllValues()
    {
        Function<Entry<Bucket<Long>, Integer>, Long[]> transformFun = new Function<Entry<Bucket<Long>, Integer>, Long[]>()
        {
            @Override
            public Long[] apply( Entry<Bucket<Long>, Integer> arg0 )
            {
                long bucketId = ( (DiscreteBucket<Long>) arg0.getKey() ).getId();
                long bucketValue = arg0.getValue();
                return new Long[] { bucketId, bucketValue };
            }
        };
        List<Long[]> sortedValues = Lists.newArrayList( Iterables.transform( measurements.getAllBuckets(), transformFun ) );
        Collections.sort( sortedValues, new ArrayFirstElementComparator() );
        return sortedValues;
    }

    public static class ArrayFirstElementComparator implements Comparator<Long[]>
    {
        @Override
        public int compare( Long[] array1, Long[] array2 )
        {
            if ( array1[0] == array2[0] ) return 0;
            if ( array1[0] > array2[0] ) return -1;
            return 1;
        }
    }
}
