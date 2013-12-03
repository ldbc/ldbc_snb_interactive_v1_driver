package com.ldbc.driver.generator;

public class ConstantGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final GENERATE_TYPE thing;

    ConstantGenerator( GENERATE_TYPE thing )
    {
        this.thing = thing;
    }

    @Override
    protected GENERATE_TYPE doNext()
    {
        return thing;
    }
}
