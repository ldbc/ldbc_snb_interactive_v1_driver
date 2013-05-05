package com.yahoo.ycsb.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.yahoo.ycsb.util.Histogram;
import com.yahoo.ycsb.util.Pair;
import com.yahoo.ycsb.util.Bucket.DiscreteBucket;

public class DiscreteMultiGeneratorVariableProbabilitiesConstantSizeTest extends GeneratorTest<Set<String>, Integer>
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
        expectedDistribution.addBucket( new DiscreteBucket<Set<String>>( s ), 0 );
        expectedDistribution.addBucket( new DiscreteBucket<Set<String>>( s1 ), 1 );
        expectedDistribution.addBucket( new DiscreteBucket<Set<String>>( s2 ), 2 );
        expectedDistribution.addBucket( new DiscreteBucket<Set<String>>( s3 ), 4 );
        expectedDistribution.addBucket( new DiscreteBucket<Set<String>>( s12 ), 0 );
        expectedDistribution.addBucket( new DiscreteBucket<Set<String>>( s13 ), 0 );
        expectedDistribution.addBucket( new DiscreteBucket<Set<String>>( s23 ), 0 );
        expectedDistribution.addBucket( new DiscreteBucket<Set<String>>( s123 ), 0 );
        return expectedDistribution;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.01;
    }

    @Override
    public Generator<Set<String>> getGeneratorImpl()
    {
        Pair<Double, String> p1 = new Pair<Double, String>( 1.0, "1" );
        Pair<Double, String> p2 = new Pair<Double, String>( 2.0, "2" );
        Pair<Double, String> p3 = new Pair<Double, String>( 4.0, "3" );
        ArrayList<Pair<Double, String>> items = new ArrayList<Pair<Double, String>>();
        items.add( p1 );
        items.add( p2 );
        items.add( p3 );
        Generator<Integer> amountToRetrieveGenerator = getGeneratorBuilder().constantGenerator( 1 ).build();
        DiscreteMultiGenerator<String> generator = getGeneratorBuilder().discreteMultiGenerator( items,
                amountToRetrieveGenerator ).build();
        return generator;
    }

    @Test( expected = GeneratorException.class )
    public void emptyConstructorTest()
    {
        // Given
        Generator<Integer> amountToRetrieveGenerator = getGeneratorBuilder().constantGenerator( 1 ).build();
        ArrayList<Pair<Double, String>> emptyItems = new ArrayList<Pair<Double, String>>();
        Generator<Set<String>> generator = getGeneratorBuilder().discreteMultiGenerator( emptyItems,
                amountToRetrieveGenerator ).build();

        // When
        generator.next();

        // Then
        assertEquals( "Empty DiscreteGenerator should throw exception on next()", false, true );
    }
}
