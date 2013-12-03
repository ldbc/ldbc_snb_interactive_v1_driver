package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

public class PrefixGeneratorWrapper extends Generator<String>
{
    private final Generator<?> generator;
    private final String prefix;

    public PrefixGeneratorWrapper( Generator<?> generator, String prefix )
    {
        this.generator = generator;
        this.prefix = prefix;
    }

    @Override
    protected String doNext() throws GeneratorException
    {
        if ( false == generator.hasNext() ) return null;
        return prefix + generator.next().toString();
    }
}
