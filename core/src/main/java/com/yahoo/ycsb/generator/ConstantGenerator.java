package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

public class ConstantGenerator<T> extends Generator<T>
{
    private final T thing;

    ConstantGenerator( RandomDataGenerator random, T thing )
    {
        super( random );
        this.thing = thing;
    }

    @Override
    protected T doNext()
    {
        return thing;
    }
}
