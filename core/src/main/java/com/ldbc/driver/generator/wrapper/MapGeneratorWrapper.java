package com.ldbc.driver.generator.wrapper;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.util.Function;

public class MapGeneratorWrapper<F, T> extends Generator<T>
{
    private final Generator<F> fromGenerator;
    private final Function<F, T> mapFunction;

    protected MapGeneratorWrapper( Generator<F> fromGenerator, Function<F, T> mapFunction )
    {
        super( null );
        this.fromGenerator = fromGenerator;
        this.mapFunction = mapFunction;
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        return ( fromGenerator.hasNext() ) ? mapFunction.apply( fromGenerator.next() ) : null;
    }
}
