package com.ldbc.driver.generator.wrapper;

import org.junit.Test;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

import static org.junit.Assert.assertEquals;

public class CappedGeneratorWrapperTest
{
    @Test
    public void shouldStopAtLimitTest()
    {
        // Given
        Generator<Integer> generator = new GeneratorFactory( new RandomDataGeneratorFactory() ).uniformNumberGenerator(
                1, 10 );
        Generator<Integer> cappedGenerator = new CappedGeneratorWrapper<Integer>( generator, 10 );

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
