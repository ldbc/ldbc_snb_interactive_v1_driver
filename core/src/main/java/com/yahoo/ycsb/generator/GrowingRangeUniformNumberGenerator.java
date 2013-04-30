package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.util.NumberHelper;

public class GrowingRangeUniformNumberGenerator<T extends Number> extends Generator<T>
{
    private final MinMaxGeneratorWrapper<T> boundingGenerator;
    private final NumberHelper<T> number;

    GrowingRangeUniformNumberGenerator( RandomDataGenerator random, MinMaxGeneratorWrapper<T> boundingGenerator )
    {
        super( random );
        this.boundingGenerator = boundingGenerator;
        this.number = NumberHelper.createNumberHelper( boundingGenerator.getMin().getClass() );
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        double min = boundingGenerator.getMin().doubleValue();
        double max = boundingGenerator.getMax().doubleValue();
        return number.round( getRandom().nextUniform( min, max ) );
    }
}
