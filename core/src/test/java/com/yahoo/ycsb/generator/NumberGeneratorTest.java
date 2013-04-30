package com.yahoo.ycsb.generator;

import java.util.List;

import org.junit.Test;

import com.yahoo.ycsb.util.NumberHelper;

import static org.junit.Assert.assertEquals;

public abstract class NumberGeneratorTest<T extends Number, C extends Number> extends GeneratorTest<T, C>
{
    public abstract double getExpectedMean();

    public abstract double getMeanTolerance();

    @Test
    public final void meanTest()
    {
        // Given
        Generator<T> generator = getGeneratorImpl();
        Double expectedMean = getExpectedMean();

        // When
        List<T> sequence = generateSequence( generator, getSampleSize() );
        Double actualMean = getSequenceMean( sequence );

        // Then
        String assertMessage = String.format(
                "Mean values should be within tolerance[%s]\nExpected mean[%s]\n Actual mean[%s]", getMeanTolerance(),
                expectedMean, actualMean );
        assertEquals( assertMessage, true, NumberHelper.withinTolerance( expectedMean, actualMean, getMeanTolerance() ) );

    }

    public final Double getSequenceMean( List<T> sequence )
    {
        int sequenceLength = sequence.size();
        double sum = 0d;
        for ( T number : sequence )
        {
            sum += number.doubleValue();
        }
        return sum / sequenceLength;
    }

}
