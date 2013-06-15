package com.ldbc.driver.generator.wrapper;

import java.util.Comparator;

import org.junit.Test;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

import static org.junit.Assert.assertEquals;

public class OrderedGeneratorWrapperTest
{
    @Test
    public void basicTest()
    {
        // Given
        int limit = 10;

        Generator<Integer> g1 = new CappedGeneratorWrapper<Integer>( new GeneratorBuilder(
                new RandomDataGeneratorFactory() ).counterGenerator( 1, 1 ).build(), limit );
        Generator<Integer> g2 = new CappedGeneratorWrapper<Integer>( new GeneratorBuilder(
                new RandomDataGeneratorFactory() ).counterGenerator( 1, 2 ).build(), limit );
        Generator<Integer> g3 = new CappedGeneratorWrapper<Integer>( new GeneratorBuilder(
                new RandomDataGeneratorFactory() ).counterGenerator( 1, 3 ).build(), limit );

        Generator<Integer> orderedGenerator = new OrderedMultiGeneratorWrapper<Integer>( new IntegerComparator(), g1,
                g2, g3 );

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
        assertEquals( 30, count );
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
