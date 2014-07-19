package com.ldbc.driver.util;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.ldbc.driver.generator.GeneratorFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GeneratorUtilsTest {
    @Test
    public void filterShouldIncludeOnly() {
        // Given
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory());
        Iterator<Integer> counterGenerator = generators.incrementing(1, 1);
        Iterator<Integer> cappedCounterGenerator = generators.limit(counterGenerator, 10);
        Integer[] includeNumbers = new Integer[]{1, 2, 3};
        Iterator<Integer> filteredCappedCounterGenerator = GeneratorFactory.includeOnly(cappedCounterGenerator,
                includeNumbers);

        // When
        List<Integer> numbers = new ArrayList<Integer>();
        while (filteredCappedCounterGenerator.hasNext()) {
            numbers.add(filteredCappedCounterGenerator.next());
        }

        // Then
        assertEquals(Arrays.asList(new Integer[]{1, 2, 3}), numbers);
    }

    @Test
    public void filterShouldExcludeAll() {
        // Given
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory());
        Iterator<Integer> counterGenerator = generators.incrementing(1, 1);
        Iterator<Integer> cappedCounterGenerator = generators.limit(counterGenerator, 10);
        Integer[] excludeNumbers = new Integer[]{1, 2, 3};
        Iterator<Integer> filteredCappedCounterGenerator = GeneratorFactory.excludeAll(cappedCounterGenerator,
                excludeNumbers);

        // When
        List<Integer> numbers = new ArrayList<Integer>();
        while (filteredCappedCounterGenerator.hasNext()) {
            numbers.add(filteredCappedCounterGenerator.next());
        }

        // Then
        assertEquals(Arrays.asList(new Integer[]{4, 5, 6, 7, 8, 9, 10}), numbers);
    }

    @Test
    public void filterShouldReturn5() {
        // Given
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory());
        Iterator<Integer> counterGenerator = generators.incrementing(1, 1);
        Iterator<Integer> cappedCounterGenerator = generators.limit(counterGenerator, 10);
        Iterator<Integer> filteredCappedCounterGenerator = Iterators.filter(cappedCounterGenerator,
                new Predicate<Integer>() {
                    @Override
                    public boolean apply(Integer input) {
                        return 5 == input;
                    }
                });

        // When
        List<Integer> numbers = new ArrayList<Integer>();
        while (filteredCappedCounterGenerator.hasNext()) {
            numbers.add(filteredCappedCounterGenerator.next());
        }

        // Then
        assertEquals(Arrays.asList(new Integer[]{5}), numbers);
    }

};
