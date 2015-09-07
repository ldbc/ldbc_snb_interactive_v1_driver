package com.ldbc.driver.generator;

import com.ldbc.driver.util.NumberHelper;

import java.util.Iterator;

import static java.lang.String.format;

public class NaiveBoundedRangeNumberGenerator<GENERATE_TYPE extends Number> extends Generator<GENERATE_TYPE>
{
    private final Integer maxIterations = 1000;
    private final MinMaxGenerator<GENERATE_TYPE> lowerBoundGenerator;
    private final MinMaxGenerator<GENERATE_TYPE> upperBoundGenerator;
    private final Iterator<GENERATE_TYPE> generator;
    private final NumberHelper<GENERATE_TYPE> number;

    NaiveBoundedRangeNumberGenerator( Iterator<GENERATE_TYPE> generator,
            MinMaxGenerator<GENERATE_TYPE> lowerBoundGenerator,
            MinMaxGenerator<GENERATE_TYPE> upperBoundGenerator )
    {
        this.lowerBoundGenerator = lowerBoundGenerator;
        this.upperBoundGenerator = upperBoundGenerator;
        this.generator = generator;
        this.number = NumberHelper.createNumberHelper( lowerBoundGenerator.getMin().getClass() );
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        GENERATE_TYPE next;
        for ( int i = 0; i < maxIterations; i++ )
        {
            next = generator.next();
            if ( number.gte( next, lowerBoundGenerator.getMin() ) && number.lte( next, upperBoundGenerator.getMax() ) )
            {
                return next;
            }
        }
        throw new GeneratorException(
                format( "Random in-range number not found within maxIterations[%s]", maxIterations )
        );
    }
}
