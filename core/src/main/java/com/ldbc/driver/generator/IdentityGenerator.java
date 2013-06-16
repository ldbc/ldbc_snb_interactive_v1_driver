package com.ldbc.driver.generator;

public class IdentityGenerator<T> extends Generator<T>
{
    private final T[] things;
    private int index = 0;

    public IdentityGenerator( T... things )
    {
        super( null );
        this.things = things;
    }

    @Override
    protected T doNext()
    {
        if ( index >= things.length )
        {
            return null;
        }
        return things[index++];
    }
}
