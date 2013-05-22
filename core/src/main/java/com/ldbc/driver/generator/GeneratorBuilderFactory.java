package com.ldbc.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.util.RandomDataGeneratorFactory;

public class GeneratorBuilderFactory
{
    final private RandomDataGenerator random;

    public GeneratorBuilderFactory( RandomDataGenerator random )
    {
        this.random = random;
    }

    public GeneratorBuilder newGeneratorBuilder()
    {
        long seed = random.nextLong( Long.MIN_VALUE, Long.MAX_VALUE );
        RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory( seed );
        return new GeneratorBuilder( randomFactory );
    }
}
