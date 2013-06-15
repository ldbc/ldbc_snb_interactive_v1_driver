package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

public class CappedGeneratorWrapper<T> extends Generator<T>
{
    private final Generator<T> generator;
    private final long limit;
    private long count = 0;

    public CappedGeneratorWrapper( Generator<T> generator, long limit )
    {
        super( null );
        this.generator = generator;
        this.limit = limit;
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        if ( count == limit ) return null;
        T next = ( generator.hasNext() ) ? generator.next() : null;
        count++;
        return next;
    }
}
