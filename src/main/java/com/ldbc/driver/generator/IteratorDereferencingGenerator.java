package com.ldbc.driver.generator;

import java.util.Iterator;

public class IteratorDereferencingGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final Iterator<Iterator<GENERATE_TYPE>> generator;

    IteratorDereferencingGenerator( Iterator<Iterator<GENERATE_TYPE>> discreteGenerator )
    {
        this.generator = discreteGenerator;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        return generator.next().next();
    }
}
