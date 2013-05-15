package com.ldbc.db2;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorException;

public abstract class OperationGenerator2<T extends Operation2> extends Generator<T>
{
    protected OperationGenerator2( RandomDataGenerator random )
    {
        super( random );
    }

    @Override
    protected abstract T doNext() throws GeneratorException;
}
