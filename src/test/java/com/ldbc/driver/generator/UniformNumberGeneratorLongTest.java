package com.ldbc.driver.generator;

import java.util.Iterator;

import com.google.common.collect.Range;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.Bucket.NumberRangeBucket;

public class UniformNumberGeneratorLongTest extends NumberGeneratorTest<Long, Long>
{
    private final long min = 0;
    private final long max = 5;

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
    public Iterator<Long> getGeneratorImpl()
    {
        return getGeneratorFactory().uniform( min, max );
    }

    @Override
    public Histogram<Long, Long> getExpectedDistribution()
    {
        Histogram<Long, Long> expectedDistribution = new Histogram<Long, Long>( 0l );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( -0.5d, 0.5d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 0.5d, 1.5d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 1.5d, 2.5d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 2.5d, 3.5d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 3.5d, 4.5d ) ), 1l );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closed( 4.5d, 5.5d ) ), 1l );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return ( (double) max - (double) min ) / 2;
    }
}
