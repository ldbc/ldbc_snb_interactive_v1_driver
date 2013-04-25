package com.yahoo.ycsb.generator;

import com.google.common.collect.Range;
import com.yahoo.ycsb.Histogram;

public class ConstantNumberGeneratorTest extends NumberGeneratorTest<Long>
{
    private final long constant = 42;

    @Override
    public double getMeanTolerance()
    {
        return 0.0;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.0;
    }

    @Override
    public Generator<Long> getGeneratorImpl()
    {
        return getGeneratorFactory().newConstantIntegerGenerator( constant );
    }

    @Override
    public Histogram<Long> getExpectedDistribution()
    {
        Histogram<Long> expectedDistribution = new Histogram<Long>( 0l );
        expectedDistribution.addBucket( Range.closedOpen( 41.99d, 42.01d ), 1l );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return 42.0;
    }
}
