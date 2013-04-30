package com.yahoo.ycsb.generator;

import org.junit.Ignore;

import com.google.common.collect.Range;
import com.yahoo.ycsb.generator.ycsb.HotspotGenerator;
import com.yahoo.ycsb.util.Histogram;
import com.yahoo.ycsb.util.Bucket.NumberRangeBucket;

// TODO the values set in this test are arbitrary 
// TODO set reasonable values and possibly more tests to test for illegal parameter handling 
@Ignore
public class HotspotGeneratorTest<T extends Number> extends NumberGeneratorTest<Long, Integer>
{
    private int lowerBound = 1;
    private int upperBound = 100;
    private double hotsetFraction = 0.1;
    private double hotOpnFraction = 0.1;

    @Override
    public double getMeanTolerance()
    {
        // TODO this is arbitrary, set reasonable value
        return 0.05;
    }

    @Override
    public double getDistributionTolerance()
    {
        // TODO this is arbitrary, set reasonable value
        return 0.05;
    }

    @Override
    public Generator<Long> getGeneratorImpl()
    {
        return getGeneratorBuilder().newHotspotGenerator( lowerBound, upperBound, hotsetFraction, hotOpnFraction ).build();
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
        HotspotGenerator generator = (HotspotGenerator) getGeneratorImpl();
        return hotOpnFraction * ( lowerBound + generator.getHotInterval() / 2.0 ) + ( 1 - hotOpnFraction )
               * ( lowerBound + generator.getHotInterval() + generator.getColdInterval() / 2.0 );
    }

}
