package com.ldbc.generator;

import java.util.List;

import com.ldbc.generator.Generator;
import com.ldbc.util.Bucket;
import com.ldbc.util.Histogram;

public class RandomByteIteratorGeneratorTest extends NumberGeneratorTest<Long, Long>
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
        return 0.0;
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
        return 0.0;
    }
}
