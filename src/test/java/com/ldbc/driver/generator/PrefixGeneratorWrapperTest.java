package com.ldbc.driver.generator;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class PrefixGeneratorWrapperTest {
    private final long RANDOM_SEED = 42;
    private GeneratorFactory generatorFactory = null;

    @Before
    public final void initGeneratorFactory() {
        generatorFactory = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    @Test
    public void shouldPrefixEveryElementInIteratorAndNothingMore() {
        Iterator<Integer> incrementing = generatorFactory.boundedIncrementing(0, 2, 10);
        Iterator<String> prefixing = generatorFactory.prefix(incrementing, "pre");
        assertThat(prefixing.next(), is("pre0"));
        assertThat(prefixing.next(), is("pre2"));
        assertThat(prefixing.next(), is("pre4"));
        assertThat(prefixing.next(), is("pre6"));
        assertThat(prefixing.next(), is("pre8"));
        assertThat(prefixing.next(), is("pre10"));
        assertThat(prefixing.hasNext(), is(false));
        assertThat(incrementing.hasNext(), is(false));
    }
}
