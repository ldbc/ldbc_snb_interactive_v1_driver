package com.ldbc.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.generator.wrapper.MinMaxGeneratorWrapper;
import com.ldbc.driver.util.NumberHelper;

public class NaiveBoundedRangeNumberGenerator<T extends Number> extends Generator<T>
{
    private final Integer maxIterations = 1000;
    private final MinMaxGeneratorWrapper<T> lowerBoundGenerator;
    private final MinMaxGeneratorWrapper<T> upperBoundGenerator;
    private final Generator<T> generator;
    private final NumberHelper<T> number;

    NaiveBoundedRangeNumberGenerator( RandomDataGenerator random, Generator<T> generator,
            MinMaxGeneratorWrapper<T> lowerBoundGenerator, MinMaxGeneratorWrapper<T> upperBoundGenerator )
    {
        super( random );
        this.lowerBoundGenerator = lowerBoundGenerator;
        this.upperBoundGenerator = upperBoundGenerator;
        this.generator = generator;
        this.number = NumberHelper.createNumberHelper( lowerBoundGenerator.getMin().getClass() );
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        T next;
        for ( int i = 0; i < maxIterations; i++ )
        {
            next = generator.next();
            if ( number.gte( next, lowerBoundGenerator.getMin() ) && number.lte( next, upperBoundGenerator.getMax() ) )
            {
                return next;
            }
        }
        throw new GeneratorException( String.format( "Random in-range number not be found within maxIterations[%s]",
                maxIterations ) );
    }
}
