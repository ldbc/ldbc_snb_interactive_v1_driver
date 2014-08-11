package com.ldbc.driver.generator;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RepeatingGeneratorTest {

    GeneratorFactory generators;

    @Before
    public void initGenerators() {
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
    }

    @Test
    public void shouldExhaustImmediatelyIfSourceGeneratorIsEmpty() {
        // Given
        Iterator<Integer> sourceGenerator = generators.identity();

        // When
        Iterator<Integer> repeatingGenerator = generators.repeating(sourceGenerator);

        // Then
        assertThat(repeatingGenerator.hasNext(), is(false));
    }

    @Test
    public void shouldLoopIndefinitely() {
        // Given
        Iterator<Integer> sourceGenerator = generators.identity(1, 2, 3);

        // When
        Iterator<Integer> repeatingGenerator = generators.repeating(sourceGenerator);

        // Then
        assertThat(repeatingGenerator.next(), is(1));
        assertThat(repeatingGenerator.next(), is(2));
        assertThat(repeatingGenerator.next(), is(3));
        assertThat(repeatingGenerator.next(), is(1));
        assertThat(repeatingGenerator.next(), is(2));
        assertThat(repeatingGenerator.next(), is(3));
        assertThat(repeatingGenerator.next(), is(1));
    }
}
