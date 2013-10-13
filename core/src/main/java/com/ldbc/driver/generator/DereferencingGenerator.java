package com.ldbc.driver.generator;

public class DereferencingGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final Generator<Generator<GENERATE_TYPE>> generator;

    DereferencingGenerator( Generator<Generator<GENERATE_TYPE>> discreteGenerator )
    {
        this.generator = discreteGenerator;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        return generator.next().next();
    }
}
