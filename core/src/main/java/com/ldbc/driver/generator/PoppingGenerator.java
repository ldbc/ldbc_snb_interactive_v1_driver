package com.ldbc.driver.generator;

import java.util.Collection;

public class PoppingGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final Generator<? extends Collection<GENERATE_TYPE>> generator;

    PoppingGenerator( Generator<? extends Collection<GENERATE_TYPE>> generator )
    {
        this.generator = generator;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        return generator.next().iterator().next();
    }
}
