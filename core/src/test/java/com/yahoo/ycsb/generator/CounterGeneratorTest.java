package com.yahoo.ycsb.generator;

import java.util.List;

import com.google.common.collect.Range;
import com.yahoo.ycsb.Histogram;

public class CounterGeneratorTest extends NumberGeneratorTest<Long>
{
    private final long start = 0;

    @Override
    public double getMeanTolerance()
    {
        return 0.0;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.001;
    }

    @Override
    public Generator<Long> getGeneratorImpl()
    {
        return getGeneratorFactory().newCounterGenerator( start );
    }

    @Override
    public Histogram<Long> getExpectedDistribution()
    {
        Histogram<Long> expectedDistribution = new Histogram<Long>( 0l );

        double min = (double) start;
        double max = (double) getSampleSize();
        int bucketCount = 10;
        List<Range<Double>> buckets = Histogram.makeEqualBucketRanges( min, max, bucketCount );
        expectedDistribution.addBuckets( buckets, 1l );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return ( getSampleSize() - 1 ) / 2.0;
    }

}
