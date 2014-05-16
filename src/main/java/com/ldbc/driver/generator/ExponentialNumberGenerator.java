package com.ldbc.driver.generator;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.util.NumberHelper;

public class ExponentialNumberGenerator<GENERATE_TYPE extends Number> extends Generator<GENERATE_TYPE>
{
    private final ExponentialDistribution exponentialDistribution;
    private final NumberHelper<GENERATE_TYPE> number;

    ExponentialNumberGenerator( RandomDataGenerator random, GENERATE_TYPE mean )
    {
        this.exponentialDistribution = new ExponentialDistribution( random.getRandomGenerator(), mean.doubleValue(),
                ExponentialDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY );
        this.number = NumberHelper.createNumberHelper( mean.getClass() );
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        return number.cast( exponentialDistribution.sample() );
    }
}
