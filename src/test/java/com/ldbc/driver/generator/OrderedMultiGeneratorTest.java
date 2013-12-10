package com.ldbc.driver.generator;

import java.util.Comparator;
import java.util.Iterator;

import org.junit.Test;

import com.ldbc.driver.generator.IdentityGenerator;
import com.ldbc.driver.generator.OrderedMultiGenerator;

import static org.junit.Assert.assertEquals;

public class OrderedMultiGeneratorTest
{
    @Test
    public void shouldFailWhenLookaheadIsTooShort()
    {
        // Given
        int lookaheadDistance = 1;
        Iterator<Integer> g1 = new IdentityGenerator<Integer>( 1, 0, 3 );

        Iterator<Integer> orderedGenerator = new OrderedMultiGenerator<Integer>( new IntegerComparator(),
                lookaheadDistance, g1 );

        // When
        int count = 0;
        int lastNumber = Integer.MIN_VALUE;
        while ( orderedGenerator.hasNext() )
        {
            count++;
            Integer number = orderedGenerator.next();
            if ( lastNumber > number )
            {
                assertEquals( 2, count );
                break;
            }
            assertEquals( true, count < 3 );
            lastNumber = number;
        }
    }

    @Test
    public void shouldOrderSingleGeneratorWhenLookaheadIsSufficient()
    {
        // Given
        int lookaheadDistance = 2;
        Iterator<Integer> g1 = new IdentityGenerator<Integer>( 1, 0, 3 );

        Iterator<Integer> orderedGenerator = new OrderedMultiGenerator<Integer>( new IntegerComparator(),
                lookaheadDistance, g1 );

        // When
        int count = 0;
        int lastNumber = Integer.MIN_VALUE;
        while ( orderedGenerator.hasNext() )
        {
            Integer number = orderedGenerator.next();
            assertEquals( true, lastNumber <= number );
            lastNumber = number;
            count++;
        }
        assertEquals( 3, count );
    }

    @Test
    public void shouldOrderMultiGeneratorsWhenLookaheadIsSufficient()
    {
        // Given
        int lookaheadDistance = 4;
        Integer[] numbers1 = new Integer[] { 1, 0, 3, 4 };
        Integer[] numbers2 = new Integer[] { 2, 4, 0, 8 };
        Integer[] numbers3 = new Integer[] { 1, 2, 3, 0 };
        Iterator<Integer> g1 = new IdentityGenerator<Integer>( numbers1 );
        Iterator<Integer> g2 = new IdentityGenerator<Integer>( numbers2 );
        Iterator<Integer> g3 = new IdentityGenerator<Integer>( numbers3 );

        Iterator<Integer> orderedGenerator = new OrderedMultiGenerator<Integer>( new IntegerComparator(),
                lookaheadDistance, g1, g2, g3 );

        // When
        int count = 0;
        int lastNumber = Integer.MIN_VALUE;
        while ( orderedGenerator.hasNext() )
        {
            Integer number = orderedGenerator.next();
            assertEquals( true, lastNumber <= number );
            lastNumber = number;
            count++;
        }

        // Then
        assertEquals( numbers1.length + numbers2.length + numbers3.length, count );
    }

    @Test
    public void shouldOrderMultiGeneratorsWhenLookaheadIsLongerThanInput()
    {
        // Given
        int lookaheadDistance = 100;
        Integer[] numbers1 = new Integer[] { 1, 0, 3, 4 };
        Integer[] numbers2 = new Integer[] { 2, 4, 0, 8 };
        Integer[] numbers3 = new Integer[] { 1, 2, 3, 0 };
        Iterator<Integer> g1 = new IdentityGenerator<Integer>( numbers1 );
        Iterator<Integer> g2 = new IdentityGenerator<Integer>( numbers2 );
        Iterator<Integer> g3 = new IdentityGenerator<Integer>( numbers3 );

        Iterator<Integer> orderedGenerator = new OrderedMultiGenerator<Integer>( new IntegerComparator(),
                lookaheadDistance, g1, g2, g3 );

        // When
        int count = 0;
        int lastNumber = Integer.MIN_VALUE;
        while ( orderedGenerator.hasNext() )
        {
            Integer number = orderedGenerator.next();
            assertEquals( true, lastNumber <= number );
            lastNumber = number;
            count++;
        }

        // Then
        assertEquals( numbers1.length + numbers2.length + numbers3.length, count );
    }

    private static class IntegerComparator implements Comparator<Integer>
    {
        @Override
        public int compare( Integer i1, Integer i2 )
        {
            return i1 - i2;
        }
    }

};
