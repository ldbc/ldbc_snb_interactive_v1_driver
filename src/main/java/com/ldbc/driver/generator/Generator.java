package com.ldbc.driver.generator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class Generator<GENERATE_TYPE> implements Iterator<GENERATE_TYPE>
{
    private GENERATE_TYPE next = null;

    // Return null if nothing more to generate
    protected abstract GENERATE_TYPE doNext() throws GeneratorException;

    public final GENERATE_TYPE next()
    {
        next = (next == null) ? doNext() : next;
        if ( null == next )
        { throw new NoSuchElementException( "Generator has nothing more to generate" ); }
        GENERATE_TYPE tempNext = next;
        next = null;
        return tempNext;
    }

    @Override
    public final boolean hasNext()
    {
        next = (next == null) ? doNext() : next;
        return (next != null);
    }

    @Override
    public final void remove()
    {
        throw new UnsupportedOperationException();
    }
}
