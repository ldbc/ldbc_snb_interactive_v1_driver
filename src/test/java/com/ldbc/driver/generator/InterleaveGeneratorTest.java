package com.ldbc.driver.generator;

import com.google.common.collect.Lists;
import com.ldbc.driver.util.Function0;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class InterleaveGeneratorTest
{

    GeneratorFactory generators;

    @Before
    public void initGenerators()
    {
        generators = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
    }

    @Test
    public void shouldBeExhaustedIfInterleaveGeneratorIsExhaustedFirstWithIntLimit()
    {
        // Given
        Iterator<Long> oneAsLongs = generators.limit( generators.constant( 1L ), 2 );
        Iterator<Integer> twoAsIntegers = generators.constant( 2 );

        // When
        Iterator<Number> interleaved = generators.<Number>interleave( oneAsLongs, twoAsIntegers, 2 );
        List<Number> interleavedList = Lists.newArrayList( interleaved );

        // Then
        assertThat( interleavedList.size(), is( 6 ) );
        assertThat( interleavedList.get( 0 ).longValue(), is( 1L ) );
        assertThat( interleavedList.get( 1 ).longValue(), is( 2L ) );
        assertThat( interleavedList.get( 2 ).longValue(), is( 2L ) );
        assertThat( interleavedList.get( 3 ).longValue(), is( 1L ) );
        assertThat( interleavedList.get( 4 ).longValue(), is( 2L ) );
        assertThat( interleavedList.get( 5 ).longValue(), is( 2L ) );
    }

    @Test
    public void shouldBeExhaustedIfInterleaveGeneratorIsExhaustedFirstWithFunctionLimit()
    {
        // Given
        Iterator<Long> oneAsLongs = generators.limit( generators.constant( 1L ), 2 );
        Iterator<Integer> twoAsIntegers = generators.constant( 2 );
        Function0<Integer,RuntimeException> amountToInterleaveFun = new Function0<Integer,RuntimeException>()
        {
            @Override
            public Integer apply()
            {
                return 2;
            }
        };

        // When
        Iterator<Number> interleaved =
                generators.<Number>interleave( oneAsLongs, twoAsIntegers, amountToInterleaveFun );
        List<Number> interleavedList = Lists.newArrayList( interleaved );

        // Then
        assertThat( interleavedList.size(), is( 6 ) );
        assertThat( interleavedList.get( 0 ).longValue(), is( 1L ) );
        assertThat( interleavedList.get( 1 ).longValue(), is( 2L ) );
        assertThat( interleavedList.get( 2 ).longValue(), is( 2L ) );
        assertThat( interleavedList.get( 3 ).longValue(), is( 1L ) );
        assertThat( interleavedList.get( 4 ).longValue(), is( 2L ) );
        assertThat( interleavedList.get( 5 ).longValue(), is( 2L ) );
    }

    @Test
    public void shouldBeExhaustedIfInterleaveGeneratorIsExhaustedFirst()
    {
        // Given
        Iterator<Long> oneAsLongs = generators.limit( generators.constant( 1L ), 2 );
        Iterator<Integer> twoAsIntegers = generators.limit( generators.constant( 2 ), 2 );

        // When
        Iterator<Number> interleaved = generators.<Number>interleave( oneAsLongs, twoAsIntegers, 2 );

        // Then
        assertThat( interleaved.hasNext(), is( true ) );
        assertThat( interleaved.next().intValue(), is( 1 ) );
        assertThat( interleaved.hasNext(), is( true ) );
        assertThat( interleaved.next().intValue(), is( 2 ) );
        assertThat( interleaved.hasNext(), is( true ) );
        assertThat( interleaved.next().intValue(), is( 2 ) );
        assertThat( interleaved.hasNext(), is( true ) );
        assertThat( interleaved.next().intValue(), is( 1 ) );
        assertThat( interleaved.hasNext(), is( false ) );
    }
}
