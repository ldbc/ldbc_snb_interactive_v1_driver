package com.ldbc.generator;

import com.google.common.collect.Range;
import com.ldbc.generator.Generator;
import com.ldbc.util.Histogram;
import com.ldbc.util.Bucket.NumberRangeBucket;

public class ExponentialGeneratorTest extends NumberGeneratorTest<Long, Integer>
{
    // TODO fix this test (see TODOs below) and/or the generator class itself

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
        return getGeneratorBuilder().exponentialGenerator( percentile, range ).build();
        // TODO test this too
        // return getGeneratorFactory().newExponentialGenerator( mean );
    }

    @Override
    public Histogram<Long, Integer> getExpectedDistribution()
    {
        Histogram<Long, Integer> expectedDistribution = new Histogram<Long, Integer>( 0 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 0d, 90d ) ), 9 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closed( 90d, Double.MAX_VALUE ) ), 1 );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return mean;
    }

}
