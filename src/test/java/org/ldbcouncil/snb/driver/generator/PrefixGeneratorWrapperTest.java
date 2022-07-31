package org.ldbcouncil.snb.driver.generator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class PrefixGeneratorWrapperTest {
    private final long RANDOM_SEED = 42;
    private GeneratorFactory generatorFactory = null;

    @BeforeEach
    public final void initGeneratorFactory() {
        generatorFactory = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    @Test
    public void shouldPrefixEveryElementInIteratorAndNothingMore() {
        Iterator<Integer> incrementing = generatorFactory.boundedIncrementing(0, 2, 10);
        Iterator<String> prefixing = generatorFactory.prefix(incrementing, "pre");
        assertEquals("pre0", prefixing.next());
        assertEquals("pre2", prefixing.next());
        assertEquals("pre4", prefixing.next());
        assertEquals("pre6", prefixing.next());
        assertEquals("pre8", prefixing.next());
        assertEquals("pre10", prefixing.next());
        assertFalse(prefixing.hasNext());
        assertFalse(incrementing.hasNext());
    }
}
