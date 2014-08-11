package com.ldbc.driver.generator;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilteringGeneratorTest {
    @Test
    public void filterShouldIncludeOnly() {
        // Given
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory());
        Iterator<Integer> counterGenerator = gf.incrementing(1, 1);
        Iterator<Integer> cappedCounterGenerator = gf.limit(counterGenerator, 10);
        Integer[] includeNumbers = new Integer[]{1, 2, 3};
        Iterator<Integer> filteredCappedCounterGenerator = gf.includeOnly(cappedCounterGenerator, includeNumbers);

        // When
        List<Integer> numbers = Lists.newArrayList(filteredCappedCounterGenerator);

        // Then
        assertEquals(Arrays.asList(new Integer[]{1, 2, 3}), numbers);
    }

    @Test
    public void filterShouldExcludeAll() {
        // Given
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory());
        Iterator<Integer> counterGenerator = gf.incrementing(1, 1);
        Iterator<Integer> cappedCounterGenerator = gf.limit(counterGenerator, 10);
        Integer[] excludeNumbers = new Integer[]{1, 2, 3};
        Iterator<Integer> filteredCappedCounterGenerator = gf.excludeAll(cappedCounterGenerator, excludeNumbers);

        // When
        List<Integer> numbers = Lists.newArrayList(filteredCappedCounterGenerator);

        // Then
        assertEquals(Arrays.asList(new Integer[]{4, 5, 6, 7, 8, 9, 10}), numbers);
    }

    @Test
    public void filterShouldReturn5() {
        // Given
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory());
        Iterator<Integer> counterGenerator = gf.incrementing(1, 1);
        Iterator<Integer> cappedCounterGenerator = gf.limit(counterGenerator, 10);
        Iterator<Integer> filteredCappedCounterGenerator = gf.includeOnly(cappedCounterGenerator,
                new Predicate<Integer>() {
                    @Override
                    public boolean apply(Integer input) {
                        return 5 == input;
                    }
                });

        // When
        List<Integer> numbers = Lists.newArrayList(filteredCappedCounterGenerator);

        // Then
        assertEquals(Arrays.asList(new Integer[]{5}), numbers);
    }

};
