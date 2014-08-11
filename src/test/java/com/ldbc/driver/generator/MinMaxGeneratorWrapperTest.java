package com.ldbc.driver.generator;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MinMaxGeneratorWrapperTest {
    private final long RANDOM_SEED = 42;
    private GeneratorFactory generatorFactory = null;

    @Before
    public final void initGeneratorFactory() {
        generatorFactory = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    @Test
    public void minMaxTest() {
        Iterator<Integer> generator = generatorFactory.incrementing(5, 1);
        MinMaxGenerator<Integer> minMax = generatorFactory.minMaxGenerator(generator, 10, 5);
        assertThat(minMax.getMin(), is(10));
        assertThat(minMax.getMax(), is(5));
        assertThat(minMax.next(), is(5));
        assertThat(minMax.next(), is(6));
        assertThat(minMax.getMin(), is(5));
        assertThat(minMax.getMax(), is(6));
    }
}
