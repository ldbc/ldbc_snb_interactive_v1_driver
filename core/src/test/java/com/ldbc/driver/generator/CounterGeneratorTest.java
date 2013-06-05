package com.ldbc.driver.generator;

import java.util.List;

import org.junit.Ignore;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;

public class CounterGeneratorTest extends NumberGeneratorTest<Long, Long>
{
    private final long start = 0;

    @Override
    public double getMeanTolerance()
    {
        return 0.001;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.001;
    }

    @Override
    public Generator<Long> getGeneratorImpl()
    {
        return getGeneratorBuilder().counterGenerator( start, 1l ).build();
    }

    @Override
    public Histogram<Long, Long> getExpectedDistribution()
    {
        Histogram<Long, Long> expectedDistribution = new Histogram<Long, Long>( 0l );

        double min = (double) start;
        double max = (double) getSampleSize();
        int bucketCount = 10;
        List<Bucket<Long>> buckets = Histogram.makeBucketsOfEqualRange( min, max, bucketCount, Long.class );
        expectedDistribution.addBuckets( buckets, 1l );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return ( getSampleSize() - 1 ) / 2.0;
    }

}
