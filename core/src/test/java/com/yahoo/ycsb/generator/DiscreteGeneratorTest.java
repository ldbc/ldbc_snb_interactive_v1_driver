package com.yahoo.ycsb.generator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.GeneratorFactory;
import com.yahoo.ycsb.generator.Pair;

public class DiscreteGeneratorTest
{

    @Test( expected = WorkloadException.class )
    public void emptyConstructorTest() throws WorkloadException
    {
        // Given
        DiscreteGenerator generator = new GeneratorFactory().buildDiscreteGenerator();

        // When
        generator.next();

        // Then
        assertEquals( "Empty DiscreteGenerator should throw exception on next()", false, true );
    }

    @Test
    public void proportionsConstructorTest() throws WorkloadException
    {
        // Given
        Pair<Double, Object> p1 = new Pair<Double, Object>( 3.0, "1" );
        Pair<Double, Object> p2 = new Pair<Double, Object>( 7.0, "2" );
        DiscreteGenerator generator1 = new GeneratorFactory().buildDiscreteGenerator( p1, p2 );
        DiscreteGenerator generator2 = new GeneratorFactory().convertToDiscreteGenerator( 3.0, "1", 7.0, "2" );

        assertCorrectProportions( generator1, p1, p2 );
        assertCorrectProportions( generator2, p1, p2 );
    }

    @Test
    public void nextLastTest() throws WorkloadException
    {
        // Given
        Pair<Double, Object> p1 = new Pair<Double, Object>( 3.0, "1" );
        Pair<Double, Object> p2 = new Pair<Double, Object>( 7.0, "2" );
        DiscreteGenerator generator = new GeneratorFactory().buildDiscreteGenerator( p1, p2 );

        // When
        Pair<Double, Object> lastP = generator.next();
        assertEquals( "last() should equal previous next()", lastP, generator.last() );
        boolean lastEqualsPreviousNext = true;
        final int generationCount = 1000000;
        for ( int i = 0; i < generationCount; i++ )
        {
            lastP = generator.next();
            lastEqualsPreviousNext = lastEqualsPreviousNext && ( lastP.equals( generator.last() ) );
        }

        // Then
        assertEquals( "last() should always equal the previous next()", true, lastEqualsPreviousNext );
    }

    public void assertCorrectProportions( DiscreteGenerator generator, Pair<Double, Object> p1, Pair<Double, Object> p2 )
            throws WorkloadException
    {
        // Given

        // When
        Pair<Double, Object> p = null;
        final int generationCount = 1000000;
        Double p1Count = 0.0;
        Double p2Count = 0.0;
        for ( int i = 0; i < generationCount; i++ )
        {
            p = generator.next();
            assertEquals( "DiscreteGenerator must output an item it was given", true, p.equals( p1 ) || p.equals( p2 ) );
            if ( p1.equals( p ) ) p1Count++;
            if ( p2.equals( p ) ) p2Count++;
        }
        p1Count = p1Count / generationCount;
        p2Count = p2Count / generationCount;

        // Then
        assertEquals( "Proportion of p1 should be approx. 30%", true, ( 0.29 < p1Count ) && ( p1Count < 0.31 ) );
        assertEquals( "Proportion of p1 should be approx. 70%", true, ( 0.69 < p2Count ) && ( p2Count < 0.71 ) );
    }

}
