package org.ldbcouncil.snb.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                assertEquals(generatorB.nextLong(0, 1000), generatorA.nextLong(0, 1000));
                assertEquals(generatorB.nextUniform(0D, 1000D), generatorA.nextUniform(0D, 1000D));
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

        assertTrue(generatorsDiffer);
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

        assertTrue(generatorsDiffer);
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
            assertEquals(doubleUniformA.next(), doubleUniformB.next());
            assertEquals(longUniformA.next(), longUniformB.next());
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

        assertTrue(generatorsDiffer);
    }
}
