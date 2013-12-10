package com.ldbc.driver.generator;

import java.util.Iterator;

import org.junit.Test;

import com.ldbc.driver.data.LimitGenerator;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

import static org.junit.Assert.assertEquals;

public class LimitGeneratorTest
{
    @Test
    public void shouldStopAtLimitTest()
    {
        // Given
        Iterator<Integer> generator = new GeneratorFactory( new RandomDataGeneratorFactory() ).uniformNumberGenerator(
                1, 10 );
        Iterator<Integer> cappedGenerator = new LimitGenerator<Integer>( generator, 10 );

        // When
        int count = 0;
        while ( cappedGenerator.hasNext() )
        {
            cappedGenerator.next();
            count++;
        }

        // Then
        assertEquals( 10, count );
    }
};
