package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.util.NumberHelper;

public class UniformNumberGenerator<T extends Number> extends Generator<T>
{
    private final T lowerBound;
    private final T upperBound;
    private final NumberHelper<T> number;

    UniformNumberGenerator( RandomDataGenerator random, T lowerBound, T upperBound )
    {
        super( random );
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.number = NumberHelper.createNumberHelper( lowerBound.getClass() );
    }

    @Override
    protected T doNext()
    {
        double min = lowerBound.doubleValue();
        double max = upperBound.doubleValue();
        return number.round( getRandom().nextUniform( min, max ) );
    }
}
