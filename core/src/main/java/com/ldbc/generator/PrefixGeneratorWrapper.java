package com.ldbc.generator;

public class PrefixGeneratorWrapper extends Generator<String>
{
    private final Generator<?> generator;
    private final String prefix;

    public PrefixGeneratorWrapper( Generator<?> generator, String prefix )
    {
        super( null );
        this.generator = generator;
        this.prefix = prefix;
    }

    @Override
    protected String doNext() throws GeneratorException
    {
        return prefix + generator.doNext().toString();
    }
}
