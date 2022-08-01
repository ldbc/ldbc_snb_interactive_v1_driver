package org.ldbcouncil.snb.driver.generator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RepeatingGeneratorTest {

    GeneratorFactory generators;

    @BeforeEach
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
        assertFalse(repeatingGenerator.hasNext());
    }

    @Test
    public void shouldLoopIndefinitely() {
        // Given
        Iterator<Integer> sourceGenerator = generators.identity(1, 2, 3);

        // When
        Iterator<Integer> repeatingGenerator = generators.repeating(sourceGenerator);

        // Then
        assertEquals(1, repeatingGenerator.next());
        assertEquals(2, repeatingGenerator.next());
        assertEquals(3, repeatingGenerator.next());
        assertEquals(1, repeatingGenerator.next());
        assertEquals(2, repeatingGenerator.next());
        assertEquals(3, repeatingGenerator.next());
        assertEquals(1, repeatingGenerator.next());
    }
}
