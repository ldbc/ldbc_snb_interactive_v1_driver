package com.yahoo.ycsb.generator;

import com.google.common.collect.Range;
import com.yahoo.ycsb.Histogram;

public class ExponentialGeneratorTest extends NumberGeneratorTest<Long>
{
    // TODO fix this test and/or the generator class itself

    private final double percentile = 90.0;
    private final double range = 100.0;
    // Calculated expected mean (taken from original Generator code)
    private final double gamma = -Math.log( 1.0 - percentile / 100.0 ) / range;
    // 43.43
    private final double mean = 1 / gamma;

    @Override
    public double getMeanTolerance()
    {
        // TODO is this tolerance too generous?
        return 0.7;
    }

    @Override
    public double getDistributionTolerance()
    {
        // TODO is this tolerance too generous?
        return 0.03;
    }

    @Override
    public Generator<Long> getGeneratorImpl()
    {
        return getGeneratorFactory().newExponentialGenerator( percentile, range );
        // TODO test this too
        // return getGeneratorFactory().newExponentialGenerator( mean );
    }

    @Override
    public Histogram<Long> getExpectedDistribution()
    {
        Histogram<Long> expectedDistribution = new Histogram<Long>( 0l );
        expectedDistribution.addBucket( Range.closedOpen( 0d, 90d ), 9l );
        expectedDistribution.addBucket( Range.closed( 90d, Double.MAX_VALUE ), 1l );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return mean;
    }

}
