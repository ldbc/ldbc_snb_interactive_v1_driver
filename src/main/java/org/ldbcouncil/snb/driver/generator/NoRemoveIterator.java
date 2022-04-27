package org.ldbcouncil.snb.driver.generator;

import java.util.Iterator;

import static java.lang.String.format;

public abstract class NoRemoveIterator<TYPE> implements Iterator<TYPE>
{
    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }
}
