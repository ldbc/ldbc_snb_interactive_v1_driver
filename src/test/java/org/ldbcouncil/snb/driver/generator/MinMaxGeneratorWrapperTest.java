package org.ldbcouncil.snb.driver.generator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MinMaxGeneratorWrapperTest {
    private final long RANDOM_SEED = 42;
    private GeneratorFactory generatorFactory = null;

    @BeforeEach
    public final void initGeneratorFactory() {
        generatorFactory = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    @Test
    public void minMaxTest() {
        Iterator<Integer> generator = generatorFactory.incrementing(5, 1);
        MinMaxGenerator<Integer> minMax = generatorFactory.minMaxGenerator(generator, 10, 5);
        assertEquals(10, minMax.getMin());
        assertEquals(5, minMax.getMax());
        assertEquals(5, minMax.next());
        assertEquals(6, minMax.next());
        assertEquals(5, minMax.getMin());
        assertEquals(6, minMax.getMax());
    }
}
