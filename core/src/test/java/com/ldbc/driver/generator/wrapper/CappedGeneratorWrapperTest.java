package com.ldbc.driver.generator.wrapper;

import org.junit.Test;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

import static org.junit.Assert.assertEquals;

public class CappedGeneratorWrapperTest
{
    @Test
    public void shouldStopAtLimitTest()
    {
        // Given
        Generator<Integer> generator = new GeneratorBuilder( new RandomDataGeneratorFactory() ).uniformNumberGenerator(
                1, 10 ).build();
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
