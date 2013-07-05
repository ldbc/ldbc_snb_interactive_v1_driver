package com.ldbc.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.util.NumberHelper;

public class IncrementingGenerator<T extends Number> extends Generator<T>
{
    private final NumberHelper<T> number;
    private final T max;
    private final Generator<T> incrementByGenerator;
    private T count;

    IncrementingGenerator( RandomDataGenerator random, T start, Generator<T> incrementByGenerator, T max )
    {
        super( random );
        this.count = start;
        this.incrementByGenerator = incrementByGenerator;
        this.max = max;
        number = NumberHelper.createNumberHelper( start.getClass() );
    }

    @Override
    protected T doNext()
    {
        if ( null != max && number.gt( count, max ) )
        {
            return null;
        }
        T next = count;
        count = number.sum( count, incrementByGenerator.next() );
        return next;
    }
}
