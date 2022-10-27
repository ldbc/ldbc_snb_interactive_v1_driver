package org.ldbcouncil.snb.driver.generator;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.util.Function0;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class InterleaveGeneratorTest
{

    GeneratorFactory generators;

    @BeforeEach
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
        assertEquals( 6, interleavedList.size());
        assertEquals( 1l, interleavedList.get( 0 ).longValue());
        assertEquals( 2l, interleavedList.get( 1 ).longValue());
        assertEquals( 2l, interleavedList.get( 2 ).longValue());
        assertEquals( 1l, interleavedList.get( 3 ).longValue());
        assertEquals( 2l, interleavedList.get( 4 ).longValue());
        assertEquals( 2l, interleavedList.get( 5 ).longValue());
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
        assertEquals( 6, interleavedList.size());
        assertEquals( 1l, interleavedList.get( 0 ).longValue());
        assertEquals( 2l, interleavedList.get( 1 ).longValue());
        assertEquals( 2l, interleavedList.get( 2 ).longValue());
        assertEquals( 1l, interleavedList.get( 3 ).longValue());
        assertEquals( 2l, interleavedList.get( 4 ).longValue());
        assertEquals( 2l, interleavedList.get( 5 ).longValue());
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
        assertTrue( interleaved.hasNext());
        assertEquals(1,  interleaved.next().intValue());
        assertTrue( interleaved.hasNext());
        assertEquals(2,  interleaved.next().intValue());
        assertTrue( interleaved.hasNext());
        assertEquals(2, interleaved.next().intValue());
        assertTrue( interleaved.hasNext());
        assertEquals(1, interleaved.next().intValue() );
        assertFalse( interleaved.hasNext());
    }
}
