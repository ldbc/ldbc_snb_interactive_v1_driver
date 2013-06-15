package com.ldbc.driver.generator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;

import org.apache.commons.math3.random.RandomDataGenerator;

public abstract class Generator<T> implements Iterator<T>
{
    private T next = null;
    private final RandomDataGenerator random;
    private final Logger logger = Logger.getLogger( getClass() );

    protected Generator( RandomDataGenerator random )
    {
        this.random = random;
    }

    // Return null if nothing more to generate
    protected abstract T doNext() throws GeneratorException;

    public final synchronized T next()
    {
        next = ( next == null ) ? doNext() : next;
        if ( null == next ) throw new NoSuchElementException( "Generator has nothing more to generate" );
        T tempNext = next;
        next = null;
        return tempNext;
    }

    @Override
    public final boolean hasNext()
    {
        next = ( next == null ) ? doNext() : next;
        return ( next != null );
    }

    @Override
    public final void remove()
    {
        throw new UnsupportedOperationException( "Iterator.remove() not supported by Generator" );
    }

    protected final RandomDataGenerator getRandom()
    {
        return random;
    }

    @Override
    public String toString()
    {
        return "Generator [next=" + next + ", random=" + random + "]";
    }

    protected final Logger getLogger()
    {
        return logger;
    }
}
