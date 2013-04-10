package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.RandomDataGeneratorFactory;

public class AbstractGeneratorFactory
{
    final private RandomDataGenerator random;

    public AbstractGeneratorFactory( RandomDataGenerator random )
    {
        this.random = random;
    }

    public GeneratorFactory newGeneratorFactory()
    {
        long seed = random.nextLong( Long.MIN_VALUE, Long.MAX_VALUE );
        RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory( seed );
        return new GeneratorFactory( randomFactory );
    }
}
