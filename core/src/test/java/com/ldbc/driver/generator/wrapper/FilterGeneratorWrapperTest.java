package com.ldbc.driver.generator.wrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.common.base.Predicate;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class FilterGeneratorWrapperTest
{
    // TODO make own version of Predicate
    @Test
    public void shouldIncludeOnly()
    {
        // Given
        Generator<Integer> counterGenerator = new GeneratorFactory( new RandomDataGeneratorFactory() ).incrementingGenerator(
                1, 1 );
        Generator<Integer> cappedCounterGenerator = new CappedGeneratorWrapper<Integer>( counterGenerator, 10 );
        Integer[] includeNumbers = new Integer[] { 1, 2, 3 };
        Generator<Integer> filteredCappedCounterGenerator = FilterGeneratorWrapper.includeOnly( cappedCounterGenerator,
                includeNumbers );

        // When
        List<Integer> numbers = new ArrayList<Integer>();
        while ( filteredCappedCounterGenerator.hasNext() )
        {
            numbers.add( filteredCappedCounterGenerator.next() );
        }

        // Then
        assertEquals( Arrays.asList( new Integer[] { 1, 2, 3 } ), numbers );
    }

    @Test
    public void shouldExcludeAll()
    {
        // Given
        Generator<Integer> counterGenerator = new GeneratorFactory( new RandomDataGeneratorFactory() ).incrementingGenerator(
                1, 1 );
        Generator<Integer> cappedCounterGenerator = new CappedGeneratorWrapper<Integer>( counterGenerator, 10 );
        Integer[] excludeNumbers = new Integer[] { 1, 2, 3 };
        Generator<Integer> filteredCappedCounterGenerator = FilterGeneratorWrapper.excludeAll( cappedCounterGenerator,
                excludeNumbers );

        // When
        List<Integer> numbers = new ArrayList<Integer>();
        while ( filteredCappedCounterGenerator.hasNext() )
        {
            numbers.add( filteredCappedCounterGenerator.next() );
        }

        // Then
        assertEquals( Arrays.asList( new Integer[] { 4, 5, 6, 7, 8, 9, 10 } ), numbers );
    }

    @Test
    public void shouldReturn5()
    {
        // Given
        Generator<Integer> counterGenerator = new GeneratorFactory( new RandomDataGeneratorFactory() ).incrementingGenerator(
                1, 1 );
        Generator<Integer> cappedCounterGenerator = new CappedGeneratorWrapper<Integer>( counterGenerator, 10 );
        Generator<Integer> filteredCappedCounterGenerator = new FilterGeneratorWrapper<Integer>(
                cappedCounterGenerator, new Predicate<Integer>()
                {
                    @Override
                    public boolean apply( Integer input )
                    {
                        return 5 == input;
                    }
                } );

        // When
        List<Integer> numbers = new ArrayList<Integer>();
        while ( filteredCappedCounterGenerator.hasNext() )
        {
            numbers.add( filteredCappedCounterGenerator.next() );
        }

        // Then
        assertEquals( Arrays.asList( new Integer[] { 5 } ), numbers );
    }

};
