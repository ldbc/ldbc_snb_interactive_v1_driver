package com.ldbc.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GeneratorFactoryDeterminismTest {

    @Test
    public void randomGeneratorsWithSameSeedShouldReturnSameNumberSequence() {
        new RandomDataGenerator();
        GeneratorFactory generatorsA = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        GeneratorFactory generatorsB = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        for (int i = 0; i < 1000; i++) {
            RandomDataGenerator generatorA = generatorsA.getRandom();
            RandomDataGenerator generatorB = generatorsB.getRandom();
            for (int j = 0; j < 1000; j++) {
                assertThat(generatorA.nextLong(0, 1000), is(generatorB.nextLong(0, 1000)));
                assertThat(generatorA.nextUniform(0D, 1000D), is(generatorB.nextUniform(0D, 1000D)));
            }
        }
    }

    @Test
    public void randomGeneratorsWithDifferentSeedShouldReturnDifferentNumberSequence() {
        RandomDataGenerator generatorA = new GeneratorFactory(new RandomDataGeneratorFactory(41L)).getRandom();
        RandomDataGenerator generatorB = new GeneratorFactory(new RandomDataGeneratorFactory(42L)).getRandom();

        boolean generatorsDiffer = false;

        for (int j = 0; j < 100; j++) {
            if (generatorA.nextLong(0, 1000) != generatorB.nextLong(0, 1000))
                generatorsDiffer = true;
        }

        assertThat(generatorsDiffer, is(true));
    }

    @Test
    public void randomGeneratorsFromSameGeneratorFactoryShouldReturnDifferentNumberSequences() {
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        RandomDataGenerator generatorA = generators.getRandom();
        RandomDataGenerator generatorB = generators.getRandom();

        boolean generatorsDiffer = false;

        for (int j = 0; j < 100; j++) {
            if (generatorA.nextLong(0, 1000) != generatorB.nextLong(0, 1000))
                generatorsDiffer = true;
        }

        assertThat(generatorsDiffer, is(true));
    }

    @Test
    public void generatorsFromDifferentGeneratorFactoriesWithSameSeedShouldReturnSameNumberSequences() {
        GeneratorFactory generatorsA = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        GeneratorFactory generatorsB = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        Iterator<Double> doubleUniformA = generatorsA.uniform(0D, 1000D);
        Iterator<Long> longUniformA = generatorsA.uniform(0L, 1000L);

        Iterator<Double> doubleUniformB = generatorsB.uniform(0D, 1000D);
        Iterator<Long> longUniformB = generatorsB.uniform(0L, 1000L);

        for (int j = 0; j < 1000; j++) {
            assertThat(doubleUniformA.next(), is(doubleUniformB.next()));
            assertThat(longUniformA.next(), is(longUniformB.next()));
        }
    }

    @Test
    public void generatorsFromSameGeneratorFactoryShouldReturnDifferentNumberSequences() {
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        boolean generatorsDiffer = false;

        for (int j = 0; j < 100; j++) {
            Iterator<Long> longUniformA = generators.uniform(0L, 1000L);
            Iterator<Long> longUniformB = generators.uniform(0L, 1000L);

            if (longUniformA.next() != longUniformB.next())
                generatorsDiffer = true;
        }

        assertThat(generatorsDiffer, is(true));
    }
}