package com.ldbc.driver.generator;

public class DiscreteValuedGenerator<T> extends Generator<T>
{
    private final DiscreteGenerator<Generator<T>> discreteGenerator;

    DiscreteValuedGenerator( DiscreteGenerator<Generator<T>> discreteGenerator )
    {
        super( null );
        this.discreteGenerator = discreteGenerator;
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        return discreteGenerator.next().next();
    }
}
