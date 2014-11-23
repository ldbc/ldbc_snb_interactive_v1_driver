package com.ldbc.driver.generator;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class LimitGeneratorTest {
    @Test
    public void shouldStopAtLimitTest() {
        // Given
        Iterator<Integer> generator = new GeneratorFactory(new RandomDataGeneratorFactory()).uniform(
                1, 10);
        Iterator<Integer> cappedGenerator = new LimitGenerator<Integer>(generator, 10);

        // When
        int count = 0;
        while (cappedGenerator.hasNext()) {
            cappedGenerator.next();
            count++;
        }

        // Then
        assertEquals(10, count);
    }
};
