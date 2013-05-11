package com.ldbc.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.util.NumberHelper;

public class GrowingRangeUniformNumberGenerator<T extends Number> extends Generator<T>
{
    private final MinMaxGeneratorWrapper<T> lowerBoundGenerator;
    private final MinMaxGeneratorWrapper<T> upperBoundGenerator;
    private final NumberHelper<T> number;

    GrowingRangeUniformNumberGenerator( RandomDataGenerator random, MinMaxGeneratorWrapper<T> lowerBoundGenerator,
            MinMaxGeneratorWrapper<T> upperBoundGenerator )
    {
        super( random );
        this.lowerBoundGenerator = lowerBoundGenerator;
        this.upperBoundGenerator = upperBoundGenerator;
        this.number = NumberHelper.createNumberHelper( lowerBoundGenerator.getMin().getClass() );
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        return number.uniform( getRandom(), lowerBoundGenerator.getMin(), upperBoundGenerator.getMax() );
    }
}
