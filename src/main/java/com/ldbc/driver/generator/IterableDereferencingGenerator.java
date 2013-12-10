package com.ldbc.driver.generator;

import java.util.Iterator;

public class IterableDereferencingGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final Iterator<? extends Iterable<GENERATE_TYPE>> generator;

    IterableDereferencingGenerator( Iterator<? extends Iterable<GENERATE_TYPE>> generator )
    {
        this.generator = generator;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        return generator.next().iterator().next();
    }
}
