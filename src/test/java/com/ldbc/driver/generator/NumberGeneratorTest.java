package com.ldbc.driver.generator;

import com.ldbc.driver.util.NumberHelper;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public abstract class NumberGeneratorTest<GENERATE_TYPE extends Number, COUNT extends Number> extends
        GeneratorTest<GENERATE_TYPE,COUNT>
{
    public abstract double getExpectedMean();

    public abstract double getMeanTolerance();

    @Test
    public final void meanTest()
    {
        // Given
        Iterator<GENERATE_TYPE> generator = getGeneratorImpl( getGeneratorFactory() );
        Double expectedMean = getExpectedMean();

        // When
        List<GENERATE_TYPE> sequence = generateSequence( generator, getSampleSize() );
        Double actualMean = getSequenceMean( sequence );

        // Then
        String assertMessage = format(
                "Mean values should be within tolerance[%s]\nExpected mean[%s]\n Actual mean[%s]", getMeanTolerance(),
                expectedMean, actualMean );
        assertEquals( assertMessage, true,
                NumberHelper.withinTolerance( expectedMean, actualMean, getMeanTolerance() ) );
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
