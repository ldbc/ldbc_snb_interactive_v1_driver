package com.ldbc.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorException;
import com.ldbc.util.Histogram;
import com.ldbc.util.Pair;
import com.ldbc.util.Bucket.DiscreteBucket;

public class DiscreteGeneratorTest extends GeneratorTest<String, Integer>
{

    @Override
    public Histogram<String, Integer> getExpectedDistribution()
    {
        Histogram<String, Integer> expectedDistribution = new Histogram<String, Integer>( 0 );
        expectedDistribution.addBucket( DiscreteBucket.create( "1" ), 1 );
        expectedDistribution.addBucket( DiscreteBucket.create( "2" ), 2 );
        expectedDistribution.addBucket( DiscreteBucket.create( "3" ), 4 );
        expectedDistribution.addBucket( DiscreteBucket.create( "4" ), 8 );
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
        Pair<Double, String> p1 = Pair.create( 1.0, "1" );
        Pair<Double, String> p2 = Pair.create( 2.0, "2" );
        Pair<Double, String> p3 = Pair.create( 4.0, "3" );
        Pair<Double, String> p4 = Pair.create( 8.0, "4" );
        ArrayList<Pair<Double, String>> items = new ArrayList<Pair<Double, String>>();
        items.add( p1 );
        items.add( p2 );
        items.add( p3 );
        items.add( p4 );
        return getGeneratorBuilder().discreteGenerator( items ).build();
    }

    @Test( expected = GeneratorException.class )
    public void emptyConstructorTest()
    {
        // Given
        ArrayList<Pair<Double, String>> emptyItems = new ArrayList<Pair<Double, String>>();
        Generator<String> generator = getGeneratorBuilder().discreteGenerator( emptyItems ).build();

        // When
        generator.next();

        // Then
        assertEquals( "Empty DiscreteGenerator should throw exception on next()", false, true );
    }
}
