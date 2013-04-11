package com.yahoo.ycsb.generator;

import static org.junit.Assert.assertEquals;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.GeneratorFactory;
import com.yahoo.ycsb.generator.Pair;

public class DiscreteGeneratorTest
{
    GeneratorFactory generatorFactory = null;

    @Before
    public void initGeneratorFactory()
    {
        generatorFactory = new AbstractGeneratorFactory( new RandomDataGenerator() ).newGeneratorFactory();
    }

    @Test( expected = GeneratorException.class )
    public void emptyConstructorTest()
    {
        // Given
        DiscreteGenerator<Object> generator = generatorFactory.newDiscreteGenerator();

        // When
        generator.next();

        // Then
        assertEquals( "Empty DiscreteGenerator should throw exception on next()", false, true );
    }

    @Test
    public void proportionsConstructorTest()
    {
        // Given
        Pair<Double, Object> p1 = new Pair<Double, Object>( 3.0, "1" );
        Pair<Double, Object> p2 = new Pair<Double, Object>( 7.0, "2" );
        DiscreteGenerator<Object> generator1 = generatorFactory.newDiscreteGenerator( p1, p2 );

        assertCorrectProportions( generator1, p1, p2 );
    }

    @Test
    public void nextLastTest()
    {
        // Given
        Pair<Double, Object> p1 = new Pair<Double, Object>( 3.0, "1" );
        Pair<Double, Object> p2 = new Pair<Double, Object>( 7.0, "2" );
        DiscreteGenerator<Object> generator = generatorFactory.newDiscreteGenerator( p1, p2 );

        // When
        Object last = generator.next();
        assertEquals( "last() should equal previous next()", last, generator.last() );
        boolean lastEqualsPreviousNext = true;
        final int generationCount = 1000000;
        for ( int i = 0; i < generationCount; i++ )
        {
            last = generator.next();
            lastEqualsPreviousNext = lastEqualsPreviousNext && ( last.equals( generator.last() ) );
        }

        // Then
        assertEquals( "last() should always equal the previous next()", true, lastEqualsPreviousNext );
    }

    public void assertCorrectProportions( DiscreteGenerator<Object> generator, Pair<Double, Object> p1,
            Pair<Double, Object> p2 )
    {
        // Given

        // When
        Object thing = null;
        // final int generationCount = 1000000;
        final int generationCount = 1000000;
        Double p1Count = 0.0;
        Double p2Count = 0.0;
        for ( int i = 0; i < generationCount; i++ )
        {
            thing = generator.next();

            assertEquals( "DiscreteGenerator must output an item it was given", true,
                    thing.equals( p1._2() ) || thing.equals( p2._2() ) );
            if ( p1._2().equals( thing ) ) p1Count++;
            if ( p2._2().equals( thing ) ) p2Count++;
        }
        p1Count = p1Count / generationCount;
        p2Count = p2Count / generationCount;

        // Then
        assertEquals( "Proportion of p1 should be approx. 30%", true, ( 0.29 < p1Count ) && ( p1Count < 0.31 ) );
        assertEquals( "Proportion of p1 should be approx. 70%", true, ( 0.69 < p2Count ) && ( p2Count < 0.71 ) );
    }

}
