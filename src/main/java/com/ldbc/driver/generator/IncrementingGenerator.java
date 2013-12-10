package com.ldbc.driver.generator;

import java.util.Iterator;

import com.ldbc.driver.util.NumberHelper;

public class IncrementingGenerator<GENERATE_TYPE extends Number> extends Generator<GENERATE_TYPE>
{
    private final NumberHelper<GENERATE_TYPE> number;
    private final GENERATE_TYPE max;
    private final Iterator<GENERATE_TYPE> incrementByGenerator;
    private GENERATE_TYPE count;

    IncrementingGenerator( GENERATE_TYPE start, Iterator<GENERATE_TYPE> incrementByGenerator, GENERATE_TYPE max )
    {
        this.count = start;
        this.incrementByGenerator = incrementByGenerator;
        this.max = max;
        this.number = NumberHelper.createNumberHelper( start.getClass() );
    }

    @Override
    protected GENERATE_TYPE doNext()
    {
        if ( null != max && number.gt( count, max ) )
        {
            return null;
        }
        GENERATE_TYPE next = count;
        count = number.sum( count, incrementByGenerator.next() );
        return next;
    }
}
