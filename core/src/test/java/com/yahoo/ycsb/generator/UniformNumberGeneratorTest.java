package com.yahoo.ycsb.generator;

import com.google.common.collect.Range;
import com.yahoo.ycsb.Histogram;

public class UniformNumberGeneratorTest extends NumberGeneratorTest<Long>
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
    public Generator<Long> getGeneratorImpl()
    {
        return getGeneratorFactory().newUniformNumberGenerator( min, max );
    }

    @Override
    public Histogram<Long> getExpectedDistribution()
    {
        Histogram<Long> expectedDistribution = new Histogram<Long>( 0l );
        expectedDistribution.addBucket( Range.closedOpen( 0d, 20d ), 1l );
        expectedDistribution.addBucket( Range.closedOpen( 20d, 40d ), 1l );
        expectedDistribution.addBucket( Range.closedOpen( 40d, 60d ), 1l );
        expectedDistribution.addBucket( Range.closedOpen( 60d, 80d ), 1l );
        expectedDistribution.addBucket( Range.closed( 80d, 100d ), 1l );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return ( (double) max - (double) min ) / 2;
    }
}
