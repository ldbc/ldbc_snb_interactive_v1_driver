package com.ldbc.driver.generator;

import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class OrderedMultiGeneratorTest {
    GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

    @Test
    public void shouldFailToSortWhenLookaheadIsTooShort() {
        // Given
        int lookaheadDistance = 1;
        Iterator<Integer> g1 = generators.identity(1, 0, 3);

        // When
        Iterator<Integer> orderedGenerator = generators.mergeSort(new IntegerComparator(), lookaheadDistance, g1);

        // Then
        assertThat(orderedGenerator.next(), is(1));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(3));
        assertThat(orderedGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldOrderSingleGeneratorWhenLookaheadIsSufficient() {
        // Given
        int lookaheadDistance = 2;
        Iterator<Integer> g1 = generators.identity(1, 0, 3);

        // When
        Iterator<Integer> orderedGenerator = generators.mergeSort(new IntegerComparator(), lookaheadDistance, g1);

        // Then
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(1));
        assertThat(orderedGenerator.next(), is(3));
        assertThat(orderedGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldOrderUnevenLengthMultiGeneratorsWhenLookaheadIsOne() {
        // Given
        int lookaheadDistance = 1;
        Iterator<Integer> g1 = generators.identity(0, 1, 3, 4);
        Iterator<Integer> g2 = generators.identity(0, 2, 4, 8);
        Iterator<Integer> g3 = generators.identity(0, 1, 2, 3);
        Iterator<Integer> g4 = generators.identity(0);
        Iterator<Integer> g5 = generators.identity(10);

        // When
        Iterator<Integer> orderedGenerator = generators.mergeSort(new IntegerComparator(), lookaheadDistance, g1, g2, g3, g4, g5);

        // Then
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(1));
        assertThat(orderedGenerator.next(), is(1));
        assertThat(orderedGenerator.next(), is(2));
        assertThat(orderedGenerator.next(), is(2));
        assertThat(orderedGenerator.next(), is(3));
        assertThat(orderedGenerator.next(), is(3));
        assertThat(orderedGenerator.next(), is(4));
        assertThat(orderedGenerator.next(), is(4));
        assertThat(orderedGenerator.next(), is(8));
        assertThat(orderedGenerator.next(), is(10));
        assertThat(orderedGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldOrderMultiGeneratorsWhenLookaheadIsSufficient() {
        // Given
        int lookaheadDistance = 4;
        Iterator<Integer> g1 = generators.identity(1, 0, 3, 4);
        Iterator<Integer> g2 = generators.identity(2, 4, 0, 8);
        Iterator<Integer> g3 = generators.identity(1, 2, 3, 0);
        Iterator<Integer> g4 = generators.identity(0);
        Iterator<Integer> g5 = generators.identity(10);

        // When
        Iterator<Integer> orderedGenerator = generators.mergeSort(new IntegerComparator(), lookaheadDistance, g1, g2, g3, g4, g5);

        // Then
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(1));
        assertThat(orderedGenerator.next(), is(1));
        assertThat(orderedGenerator.next(), is(2));
        assertThat(orderedGenerator.next(), is(2));
        assertThat(orderedGenerator.next(), is(3));
        assertThat(orderedGenerator.next(), is(3));
        assertThat(orderedGenerator.next(), is(4));
        assertThat(orderedGenerator.next(), is(4));
        assertThat(orderedGenerator.next(), is(8));
        assertThat(orderedGenerator.next(), is(10));
        assertThat(orderedGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldOrderMultiGeneratorsWhenLookaheadIsLongerThanInput() {
        // Given
        int lookaheadDistance = 100;
        Iterator<Integer> g1 = generators.identity(1, 0, 3, 4);
        Iterator<Integer> g2 = generators.identity(2, 4, 0, 8);
        Iterator<Integer> g3 = generators.identity(1, 2, 3, 0);

        // When
        Iterator<Integer> orderedGenerator = generators.mergeSort(new IntegerComparator(), lookaheadDistance, g1, g2, g3);

        // Then
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(0));
        assertThat(orderedGenerator.next(), is(1));
        assertThat(orderedGenerator.next(), is(1));
        assertThat(orderedGenerator.next(), is(2));
        assertThat(orderedGenerator.next(), is(2));
        assertThat(orderedGenerator.next(), is(3));
        assertThat(orderedGenerator.next(), is(3));
        assertThat(orderedGenerator.next(), is(4));
        assertThat(orderedGenerator.next(), is(4));
        assertThat(orderedGenerator.next(), is(8));
        assertThat(orderedGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldOrderManyGenerators() {
        // Given
        int lookaheadDistance = 1;
        int generatorCount = 1000;
        Iterator<Integer>[] allGenerators = new Iterator[generatorCount];

        // When
        for (int i = 0; i < generatorCount; i++) {
            allGenerators[i] = generators.identity(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        }
        Iterator<Integer> orderedGenerator = generators.mergeSort(new IntegerComparator(), lookaheadDistance, allGenerators);

        // Then
        Integer previous = Integer.MIN_VALUE;
        while (orderedGenerator.hasNext()) {
            Integer current = orderedGenerator.next();
            assertThat(current >= previous, is(true));
            previous = current;
        }
    }

    @Test
    public void shouldOrderTime() {
        // Given
        int generatorCount = 1000;
        Iterator<Long>[] allGenerators = new Iterator[generatorCount];

        // When
        for (int i = 0; i < generatorCount; i++) {
            allGenerators[i] = generators.identity(0l, 1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l);
        }
        Iterator<Long> orderedGenerator = generators.mergeSortNumbers(allGenerators);

        // Then
        long previous = 0l;
        while (orderedGenerator.hasNext()) {
            long current = orderedGenerator.next();
            assertThat(current >= previous, is(true));
            previous = current;
        }
    }

    private static class IntegerComparator implements Comparator<Integer> {
        @Override
        public int compare(Integer i1, Integer i2) {
            return i1 - i2;
        }
    }

};
