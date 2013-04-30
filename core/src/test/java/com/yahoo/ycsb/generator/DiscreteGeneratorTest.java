package com.yahoo.ycsb.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import com.yahoo.ycsb.util.Histogram;
import com.yahoo.ycsb.util.Pair;
import com.yahoo.ycsb.util.Bucket.DiscreteBucket;

public class DiscreteGeneratorTest extends GeneratorTest<String, Integer>
{

    @Override
    public Histogram<String, Integer> getExpectedDistribution()
    {
        Histogram<String, Integer> expectedDistribution = new Histogram<String, Integer>( 0 );
        expectedDistribution.addBucket( new DiscreteBucket<String>( "1" ), 1 );
        expectedDistribution.addBucket( new DiscreteBucket<String>( "2" ), 2 );
        expectedDistribution.addBucket( new DiscreteBucket<String>( "3" ), 4 );
        expectedDistribution.addBucket( new DiscreteBucket<String>( "4" ), 8 );
        return expectedDistribution;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.01;
    }

    @Override
    public Generator<String> getGeneratorImpl()
    {
        Pair<Double, String> p1 = new Pair<Double, String>( 1.0, "1" );
        Pair<Double, String> p2 = new Pair<Double, String>( 2.0, "2" );
        Pair<Double, String> p3 = new Pair<Double, String>( 4.0, "3" );
        Pair<Double, String> p4 = new Pair<Double, String>( 8.0, "4" );
        ArrayList<Pair<Double, String>> items = new ArrayList<Pair<Double, String>>();
        items.add( p1 );
        items.add( p2 );
        items.add( p3 );
        items.add( p4 );
        return getGeneratorBuilder().newDiscreteGenerator( items ).build();
    }

    @Test( expected = GeneratorException.class )
    public void emptyConstructorTest()
    {
        // Given
        ArrayList<Pair<Double, String>> emptyItems = new ArrayList<Pair<Double, String>>();
        Generator<String> generator = getGeneratorBuilder().newDiscreteGenerator( emptyItems ).build();

        // When
        generator.next();

        // Then
        assertEquals( "Empty DiscreteGenerator should throw exception on next()", false, true );
    }
}
