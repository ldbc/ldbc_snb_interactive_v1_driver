package com.yahoo.ycsb.generator;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;

public class CounterGeneratorTest extends NumberGeneratorTest
{
    private final int start = 0;

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
    public Generator<? extends Number> getGeneratorImpl()
    {
        return getGeneratorFactory().newCounterGenerator( start );
    }

    @Override
    public Map<Range<Double>, Double> getExpectedDistribution()
    {
        List<Range<Double>> bucketList = GeneratorTestUtils.makeEqualBucketRanges( (double) start,
                (double) getSampleSize(), 10 );
        Map<Range<Double>, Double> uniformlyFilledBuckets = GeneratorTestUtils.fillBucketsUniformly( bucketList );
        return uniformlyFilledBuckets;
    }

    @Override
    public Double getExpectedMean()
    {
        return ( getSampleSize() - 1 ) / 2.0;
    }

}
