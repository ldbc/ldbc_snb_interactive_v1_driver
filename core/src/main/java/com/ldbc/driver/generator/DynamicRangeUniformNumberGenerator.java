package com.ldbc.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.generator.wrapper.MinMaxGeneratorWrapper;
import com.ldbc.driver.util.NumberHelper;

public class DynamicRangeUniformNumberGenerator<GENERATE_TYPE extends Number> extends Generator<GENERATE_TYPE>
{
    private final MinMaxGeneratorWrapper<GENERATE_TYPE> lowerBoundGenerator;
    private final MinMaxGeneratorWrapper<GENERATE_TYPE> upperBoundGenerator;
    private final NumberHelper<GENERATE_TYPE> number;
    private final RandomDataGenerator random;

    DynamicRangeUniformNumberGenerator( RandomDataGenerator random,
            MinMaxGeneratorWrapper<GENERATE_TYPE> lowerBoundGenerator,
            MinMaxGeneratorWrapper<GENERATE_TYPE> upperBoundGenerator )
    {
        this.random = random;
        this.lowerBoundGenerator = lowerBoundGenerator;
        this.upperBoundGenerator = upperBoundGenerator;
        this.number = NumberHelper.createNumberHelper( lowerBoundGenerator.getMin().getClass() );
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        return number.uniform( random, lowerBoundGenerator.getMin(), upperBoundGenerator.getMax() );
    }
}
