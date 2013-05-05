package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.util.Histogram;

public class GrowingRangeExponentialNumberGeneratorTest extends NumberGeneratorTest<Long, Long>
{
    @Override
    public double getMeanTolerance()
    {
        return 0;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0;
    }

    @Override
    public Generator<Long> getGeneratorImpl()
    {
        return null;
    }

    @Override
    public Histogram<Long, Long> getExpectedDistribution()
    {
        return null;
    }

    @Override
    public double getExpectedMean()
    {
        return 0;
    }

}
