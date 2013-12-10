package com.ldbc.driver.generator;

import java.util.Iterator;


public class PrefixGenerator extends Generator<String>
{
    private final Iterator<?> generator;
    private final String prefix;

    public PrefixGenerator( Iterator<?> generator, String prefix )
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
