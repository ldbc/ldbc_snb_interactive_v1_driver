package com.ldbc.generator.ycsb;

import com.google.common.collect.Range;
import com.ldbc.generator.Generator;
import com.ldbc.generator.NumberGeneratorTest;
import com.ldbc.util.Histogram;
import com.ldbc.util.Bucket.NumberRangeBucket;

public class YcsbDynamicRangeHotspotGeneratorTest extends NumberGeneratorTest<Long, Integer>
{
    private int lowerBound = 1;
    private int upperBound = 10;
    private double hotSetFraction = 0.2;
    private double hotOperationFraction = 0.6;

    @Override
    public double getMeanTolerance()
    {
        return 0.05;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.01;
    }

    @Override
    public Generator<Long> getGeneratorImpl()
    {
        return getGeneratorBuilder().dynamicRangeHotspotGenerator( lowerBound, upperBound, hotSetFraction,
                hotOperationFraction ).build();
    }

    @Override
    public Histogram<Long, Integer> getExpectedDistribution()
    {
        Histogram<Long, Integer> expectedDistribution = new Histogram<Long, Integer>( 0 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 0.5d, 1.5d ) ), 30 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 1.5d, 2.5d ) ), 30 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 2.5d, 3.5d ) ), 5 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 3.5d, 4.5d ) ), 5 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 4.5d, 5.5d ) ), 5 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 5.5d, 6.5d ) ), 5 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 6.5d, 7.5d ) ), 5 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 7.5d, 8.5d ) ), 5 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 8.5d, 9.5d ) ), 5 );
        expectedDistribution.addBucket( new NumberRangeBucket<Long>( Range.closedOpen( 9.5d, 10.5d ) ), 5 );
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean()
    {
        return 3.5;
    }

}
