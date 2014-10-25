package com.ldbc.driver.generator;

import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

public class RandomDataGeneratorFactory {

    private final TimeSource timeSource = new SystemTimeSource();
    private final RandomGenerator random;

    /**
     * Will use current time to seed a RandomGenerator, which will generate seeds for all returned RandomDataGenerator instances
     */
    public RandomDataGeneratorFactory() {
        this.random = getRandomGenerator(timeSource.nowAsMilli());
    }

    /**
     * Will use provided long seed to seeds all returned RandomDataGenerator instances
     *
     * @param seed
     */
    public RandomDataGeneratorFactory(Long seed) {
        this.random = getRandomGenerator(seed);
    }

    public RandomDataGenerator newRandom(long seed) {
        return new RandomDataGenerator(getRandomGenerator(seed));
    }

    public RandomDataGenerator newRandom() {
        return new RandomDataGenerator(getRandomGenerator(getSeed()));
    }

    private RandomGenerator getRandomGenerator(long seed) {
        /* 
         * From Docs: http://commons.apache.org/proper/commons-math/javadocs/api-3.2/index.html
         * If no RandomGenerator is provided in the constructor, the default is to use a Well19937c generator
         */
        return new Well19937c(seed);
    }

    private Long getSeed() {
        return random.nextLong();
    }

}
