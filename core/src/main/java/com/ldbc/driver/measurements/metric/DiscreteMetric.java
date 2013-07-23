package com.ldbc.driver.measurements.metric;

import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.Pair;

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
    public long getCount()
    {
        return measurements.sumOfAllBucketValues();
    }

    public Iterator<Pair<Long, Integer>> getAllValues()
    {
        Function<Entry<Bucket<Long>, Integer>, Pair<Long, Integer>> transformFun = new Function<Entry<Bucket<Long>, Integer>, Pair<Long, Integer>>()
        {
            @Override
            public Pair<Long, Integer> apply( Entry<Bucket<Long>, Integer> arg0 )
            {
                Long bucketId = ( (DiscreteBucket<Long>) arg0.getKey() ).getId();
                Integer bucketValue = arg0.getValue();
                return Pair.create( bucketId, bucketValue );
            }
        };
        return Iterators.transform( measurements.getAllBuckets().iterator(), transformFun );
    }
}
