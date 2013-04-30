package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.util.NumberHelper;

public class CounterGenerator<T extends Number> extends Generator<T>
{
    private final NumberHelper<T> number;
    private final T incrementBy;
    private T counter;

    CounterGenerator( RandomDataGenerator random, T start, T incrementBy )
    {
        super( random );
        counter = start;
        this.incrementBy = incrementBy;
        number = NumberHelper.createNumberHelper( start.getClass() );
    }

    @Override
    protected T doNext()
    {
        T next = counter;
        counter = number.sum( counter, incrementBy );
        return next;
    }
}
