package com.ldbc.driver.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.LimitGenerator;
import com.ldbc.driver.util.GeneratorUtils;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class GeneratorUtilsTest
{
    @Test
    public void filterShouldIncludeOnly()
    {
        // Given
        Iterator<Integer> counterGenerator = new GeneratorFactory( new RandomDataGeneratorFactory() ).incrementing(
                1, 1 );
        Iterator<Integer> cappedCounterGenerator = new LimitGenerator<Integer>( counterGenerator, 10 );
        Integer[] includeNumbers = new Integer[] { 1, 2, 3 };
        Iterator<Integer> filteredCappedCounterGenerator = GeneratorUtils.includeOnly( cappedCounterGenerator,
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
    public void filterShouldExcludeAll()
    {
        // Given
        Iterator<Integer> counterGenerator = new GeneratorFactory( new RandomDataGeneratorFactory() ).incrementing(
                1, 1 );
        Iterator<Integer> cappedCounterGenerator = new LimitGenerator<Integer>( counterGenerator, 10 );
        Integer[] excludeNumbers = new Integer[] { 1, 2, 3 };
        Iterator<Integer> filteredCappedCounterGenerator = GeneratorUtils.excludeAll( cappedCounterGenerator,
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
    public void filterShouldReturn5()
    {
        // Given
        Iterator<Integer> counterGenerator = new GeneratorFactory( new RandomDataGeneratorFactory() ).incrementing(
                1, 1 );
        Iterator<Integer> cappedCounterGenerator = new LimitGenerator<Integer>( counterGenerator, 10 );
        Iterator<Integer> filteredCappedCounterGenerator = Iterators.filter( cappedCounterGenerator,
                new Predicate<Integer>()
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
