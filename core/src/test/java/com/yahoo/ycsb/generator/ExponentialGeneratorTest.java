package com.yahoo.ycsb.generator;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Range;

public class ExponentialGeneratorTest extends NumberGeneratorTest
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
    public Generator<? extends Number> getGeneratorImpl()
    {
        return getGeneratorFactory().newExponentialGenerator( percentile, range );
        // TODO test this too
        // return getGeneratorFactory().newExponentialGenerator( mean );
    }

    @Override
    public Map<Range<Double>, Double> getExpectedDistribution()
    {
        Map<Range<Double>, Double> expectedBuckets = new HashMap<Range<Double>, Double>();
        expectedBuckets.put( Range.closedOpen( 0d, 90d ), 0.9 );
        expectedBuckets.put( Range.closed( 90d, Double.MAX_VALUE ), 0.1 );
        return expectedBuckets;
    }

    @Override
    public Double getExpectedMean()
    {
        return mean;
    }

}
