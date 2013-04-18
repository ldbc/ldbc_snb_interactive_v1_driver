package com.yahoo.ycsb.generator;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Range;

public class UniformNumberGeneratorTest extends NumberGeneratorTest
{
    private final long min = 0;
    private final long max = 100;

    @Override
    public double getMeanTolerance()
    {
        return 0.1;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.01;
    }

    @Override
    public Generator<? extends Number> getGeneratorImpl()
    {
        return getGeneratorFactory().newUniformNumberGenerator( min, max );
    }

    @Override
    public Map<Range<Double>, Double> getExpectedDistribution()
    {
        Map<Range<Double>, Double> expectedBuckets = new HashMap<Range<Double>, Double>();
        expectedBuckets.put( Range.closedOpen( 0d, 20d ), 0.2 );
        expectedBuckets.put( Range.closedOpen( 20d, 40d ), 0.2 );
        expectedBuckets.put( Range.closedOpen( 40d, 60d ), 0.2 );
        expectedBuckets.put( Range.closedOpen( 60d, 80d ), 0.2 );
        expectedBuckets.put( Range.closed( 80d, 100d ), 0.2 );
        return expectedBuckets;
    }

    @Override
    public Double getExpectedMean()
    {
        return ( (double) max - (double) min ) / 2;
    }
}
