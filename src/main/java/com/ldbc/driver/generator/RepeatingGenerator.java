package com.ldbc.driver.generator;

import com.google.common.collect.Lists;

import java.util.Iterator;

public class RepeatingGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final Iterable<GENERATE_TYPE> generatorIterable;
    private Iterator<GENERATE_TYPE> generator;

    RepeatingGenerator( Iterator<GENERATE_TYPE> generator )
    {
        this.generatorIterable = Lists.newArrayList( generator );
        this.generator = this.generatorIterable.iterator();
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        if ( generator.hasNext() )
        {
            return generator.next();
        }
        else
        {
            generator = generatorIterable.iterator();
            return (generator.hasNext()) ? generator.next() : null;
        }
    }
}
