package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

public class ConstantNumberGenerator<T extends Number> extends Generator<T>
{
    private final T constantNumber;

    ConstantNumberGenerator( RandomDataGenerator random, T number )
    {
        super( random );
        this.constantNumber = number;
    }

    @Override
    protected T doNext()
    {
        return constantNumber;
    }
}
