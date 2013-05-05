package com.ldbc.generator;

import com.google.common.collect.Range;
import com.ldbc.generator.Generator;
import com.ldbc.util.Histogram;
import com.ldbc.util.Bucket.NumberRangeBucket;

public class UniformNumberGeneratorDoubleTest extends NumberGeneratorTest<Double, Long>
{
    private final double min = 0d;
    private final double max = 5d;

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
    public Generator<Double> getGeneratorImpl()
    {
        return getGeneratorBuilder().uniformNumberGenerator( min, max ).build();
    }

    @Override
    public Histogram<Double, Long> getExpectedDistribution()
    {
        Histogram<Double, Long> expectedDistribution = new Histogram<Double, Long>( 0l );
        expectedDistribution.addBucket( new NumberRangeBucket<Double>( Range.closedOpen( 0d, 1d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Double>( Range.closedOpen( 1d, 2d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Double>( Range.closedOpen( 2d, 3d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Double>( Range.closedOpen( 3d, 4d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Double>( Range.closedOpen( 4d, 5d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Double>( Range.closed( 5d, 6d ) ), 0l );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return ( (double) max - (double) min ) / 2;
    }
}
