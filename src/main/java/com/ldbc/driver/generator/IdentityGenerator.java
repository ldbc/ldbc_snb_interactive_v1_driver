package com.ldbc.driver.generator;

public class IdentityGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final GENERATE_TYPE[] things;
    private int index = 0;

    IdentityGenerator( GENERATE_TYPE... things )
    {
        this.things = things;
    }

    @Override
    protected GENERATE_TYPE doNext()
    {
        if ( index >= things.length )
        {
            return null;
        }
        return things[index++];
    }
}
