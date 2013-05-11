package com.ldbc.generator;

import com.ldbc.generator.Generator;
import com.ldbc.util.Histogram;

public class BoundedRangeExponentialNumberGeneratorBoundedTest extends NumberGeneratorTest<Long, Long>
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
