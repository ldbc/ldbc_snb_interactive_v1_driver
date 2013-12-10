package com.ldbc.driver.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;

public class DiscreteSetGeneratorConstantProbabilitiesVariableSizeTest extends GeneratorTest<Set<String>, Integer>
{

    @Override
    public Histogram<Set<String>, Integer> getExpectedDistribution()
    {
        Set<String> s = new HashSet<String>();
        Set<String> s1 = new HashSet<String>( Arrays.asList( new String[] { "1" } ) );
        Set<String> s2 = new HashSet<String>( Arrays.asList( new String[] { "2" } ) );
        Set<String> s3 = new HashSet<String>( Arrays.asList( new String[] { "3" } ) );
        Set<String> s12 = new HashSet<String>( Arrays.asList( new String[] { "1", "2" } ) );
        Set<String> s13 = new HashSet<String>( Arrays.asList( new String[] { "1", "3" } ) );
        Set<String> s23 = new HashSet<String>( Arrays.asList( new String[] { "2", "3" } ) );
        Set<String> s123 = new HashSet<String>( Arrays.asList( new String[] { "1", "2", "3" } ) );
        Histogram<Set<String>, Integer> expectedDistribution = new Histogram<Set<String>, Integer>( 0 );
        expectedDistribution.addBucket( DiscreteBucket.create( s ), 3 );
        expectedDistribution.addBucket( DiscreteBucket.create( s1 ), 1 );
        expectedDistribution.addBucket( DiscreteBucket.create( s2 ), 1 );
        expectedDistribution.addBucket( DiscreteBucket.create( s3 ), 1 );
        expectedDistribution.addBucket( DiscreteBucket.create( s12 ), 1 );
        expectedDistribution.addBucket( DiscreteBucket.create( s13 ), 1 );
        expectedDistribution.addBucket( DiscreteBucket.create( s23 ), 1 );
        expectedDistribution.addBucket( DiscreteBucket.create( s123 ), 3 );
        return expectedDistribution;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.01;
    }

    @Override
    public Iterator<Set<String>> getGeneratorImpl()
    {
        Tuple2<Double, String> p1 = Tuple.tuple2( 1.0, "1" );
        Tuple2<Double, String> p2 = Tuple.tuple2( 1.0, "2" );
        Tuple2<Double, String> p3 = Tuple.tuple2( 1.0, "3" );
        ArrayList<Tuple2<Double, String>> items = new ArrayList<Tuple2<Double, String>>();
        items.add( p1 );
        items.add( p2 );
        items.add( p3 );
        Iterator<Integer> amountToRetrieveGenerator = getGeneratorFactory().uniformNumberGenerator( 0, 3 );
        Iterator<Set<String>> generator = getGeneratorFactory().weightedDiscreteSetGenerator( items,
                amountToRetrieveGenerator );
        return generator;
    }

    @Test( expected = GeneratorException.class )
    public void emptyConstructorTest()
    {
        // Given
        Iterator<Integer> amountToRetrieveGenerator = getGeneratorFactory().uniformNumberGenerator( 0, 3 );
        ArrayList<Tuple2<Double, String>> emptyItems = new ArrayList<Tuple2<Double, String>>();
        Iterator<Set<String>> generator = getGeneratorFactory().weightedDiscreteSetGenerator( emptyItems,
                amountToRetrieveGenerator );

        // When
        generator.next();

        // Then
        assertEquals( "Empty DiscreteGenerator should throw exception on next()", false, true );
    }
}
