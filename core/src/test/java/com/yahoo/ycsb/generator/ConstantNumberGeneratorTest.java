package com.yahoo.ycsb.generator;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Range;

public class ConstantNumberGeneratorTest extends NumberGeneratorTest
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
    public Generator<? extends Number> getGeneratorImpl()
    {
        return getGeneratorFactory().newConstantIntegerGenerator( constant );
    }

    @Override
    public Map<Range<Double>, Double> getExpectedDistribution()
    {
        Map<Range<Double>, Double> expectedBuckets = new HashMap<Range<Double>, Double>();
        expectedBuckets.put( Range.closedOpen( 41.99d, 42.01d ), 1.0 );
        return expectedBuckets;
    }

    @Override
    public Double getExpectedMean()
    {
        return 42.0;
    }

}
