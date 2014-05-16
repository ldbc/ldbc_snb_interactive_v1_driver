package com.ldbc.driver.generator;

import java.util.Iterator;


public class LimitGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final Iterator<GENERATE_TYPE> generator;
    private final long limit;
    private long count = 0;

    LimitGenerator( Iterator<GENERATE_TYPE> generator, long limit )
    {
        this.generator = generator;
        this.limit = limit;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        if ( count == limit ) return null;
        GENERATE_TYPE next = ( generator.hasNext() ) ? generator.next() : null;
        count++;
        return next;
    }
}
