package com.ldbc.driver.generator;

import com.ldbc.driver.generator.wrapper.MinMaxGeneratorWrapper;
import com.ldbc.driver.util.NumberHelper;

public class NaiveBoundedRangeNumberGenerator<GENERATE_TYPE extends Number> extends Generator<GENERATE_TYPE>
{
    private final Integer maxIterations = 1000;
    private final MinMaxGeneratorWrapper<GENERATE_TYPE> lowerBoundGenerator;
    private final MinMaxGeneratorWrapper<GENERATE_TYPE> upperBoundGenerator;
    private final Generator<GENERATE_TYPE> generator;
    private final NumberHelper<GENERATE_TYPE> number;

    NaiveBoundedRangeNumberGenerator( Generator<GENERATE_TYPE> generator,
            MinMaxGeneratorWrapper<GENERATE_TYPE> lowerBoundGenerator,
            MinMaxGeneratorWrapper<GENERATE_TYPE> upperBoundGenerator )
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
        throw new GeneratorException( String.format( "Random in-range number not found within maxIterations[%s]",
                maxIterations ) );
    }
}
