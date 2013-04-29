package com.yahoo.ycsb.generator;

import java.util.List;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.ycsb.Histogram;
import com.yahoo.ycsb.NumberHelper;

import static org.junit.Assert.assertEquals;

public abstract class NumberGeneratorTest<T extends Number, C extends Number>
{
    private final int SAMPLE_SIZE = 1000000;
    private GeneratorFactory generatorFactory = null;

    public abstract double getExpectedMean();

    public abstract double getMeanTolerance();

    public abstract Histogram<T, C> getExpectedDistribution();

    public abstract double getDistributionTolerance();

    public abstract Generator<T> getGeneratorImpl();

    protected final int getSampleSize()
    {
        return SAMPLE_SIZE;
    }

    protected final GeneratorFactory getGeneratorFactory()
    {
        return generatorFactory;
    }

    @Before
    public void initGeneratorFactory()
    {
        generatorFactory = new AbstractGeneratorFactory( new RandomDataGenerator() ).newGeneratorFactory();
    }

    @Test
    public void distributionTest()
    {
        // Given
        Generator<T> generator = getGeneratorImpl();
        Histogram<T, C> expectedDistribution = getExpectedDistribution();

        // When
        List<T> generatedSequence = GeneratorTestUtils.makeSequence( generator, getSampleSize() );
        Histogram<T, C> generatedDistribution = getExpectedDistribution();
        NumberHelper<C> number = NumberHelper.createNumberHelper( generatedDistribution.getDefaultBucketValue().getClass() );
        generatedDistribution.setAllBucketValues( number.zero() );
        generatedDistribution.importValueSequence( generatedSequence );

        // Then
        assertEquals(
                "Expected and generated distributions should be equal (with tolerance)",
                true,
                generatedDistribution.toPercentageValues().equalsWithinTolerance(
                        expectedDistribution.toPercentageValues(), getDistributionTolerance() ) );
    }

    @Test
    public void meanTest()
    {
        // Given
        Generator<? extends Number> generator = getGeneratorImpl();
        Double expectedMean = getExpectedMean();

        // When
        List<? extends Number> sequence = GeneratorTestUtils.makeSequence( generator, getSampleSize() );
        Double actualMean = GeneratorTestUtils.getSequenceMean( sequence );

        // Then
        String assertMessage = String.format( "Expected mean(%s) should equal actual mean(%s) within tolerance(%s)",
                expectedMean, actualMean, getMeanTolerance() );
        assertEquals( assertMessage, true, NumberHelper.withinTolerance( expectedMean, actualMean, getMeanTolerance() ) );

    }

    @Test
    public void nextLastTest()
    {
        // Given
        Generator<? extends Number> generator = getGeneratorImpl();

        // When

        // Then
        GeneratorTestUtils.assertLastEqualsLastNext( generator, getSampleSize() );
    }
}
