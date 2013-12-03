package com.ldbc.driver.generator;

import java.util.List;

import org.junit.Test;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.util.NumberHelper;

import static org.junit.Assert.assertEquals;

public abstract class NumberGeneratorTest<GENERATE_TYPE extends Number, COUNT extends Number> extends
        GeneratorTest<GENERATE_TYPE, COUNT>
{
    public abstract double getExpectedMean();

    public abstract double getMeanTolerance();

    @Test
    public final void meanTest()
    {
        // Given
        Generator<GENERATE_TYPE> generator = getGeneratorImpl();
        Double expectedMean = getExpectedMean();

        // When
        List<GENERATE_TYPE> sequence = generateSequence( generator, getSampleSize() );
        Double actualMean = getSequenceMean( sequence );

        // Then
        String assertMessage = String.format(
                "Mean values should be within tolerance[%s]\nExpected mean[%s]\n Actual mean[%s]", getMeanTolerance(),
                expectedMean, actualMean );
        assertEquals( assertMessage, true, NumberHelper.withinTolerance( expectedMean, actualMean, getMeanTolerance() ) );

    }

    public final Double getSequenceMean( List<GENERATE_TYPE> sequence )
    {
        int sequenceLength = sequence.size();
        double sum = 0d;
        for ( GENERATE_TYPE number : sequence )
        {
            sum += number.doubleValue();
        }
        return sum / sequenceLength;
    }

}
