package com.yahoo.ycsb.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Range;

public abstract class NumberGeneratorTest
{
    private final int SAMPLE_SIZE = 1000000;
    private GeneratorFactory generatorFactory = null;
    private List<Range<Double>> bucketRanges = null;

    public abstract double getMeanTolerance();

    public abstract double getDistributionTolerance();

    public abstract Generator<? extends Number> getGeneratorImpl();

    public abstract Map<Range<Double>, Double> getExpectedDistribution();

    public abstract Double getExpectedMean();

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
        bucketRanges = new ArrayList<Range<Double>>( getExpectedDistribution().keySet() );
        Collections.sort( bucketRanges, new BucketComparator() );
    }

    @Test
    public void distributionTest()
    {
        // Given
        Generator<? extends Number> generator = getGeneratorImpl();
        Map<Range<Double>, Double> expectedBuckets = getExpectedDistribution();

        // When
        List<? extends Number> generatedSequence = GeneratorTestUtils.makeSequence( generator, SAMPLE_SIZE );
        Map<Range<Double>, Double> generatedBuckets = GeneratorTestUtils.sequenceToBuckets( generatedSequence,
                bucketRanges );

        // Then
        GeneratorTestUtils.assertDistributionCorrect( expectedBuckets, generatedBuckets, getDistributionTolerance() );
    }

    @Test
    public void meanTest()
    {
        // Given
        Generator<? extends Number> generator = getGeneratorImpl();
        double expectedMean = getExpectedMean();

        // When
        List<? extends Number> sequence = GeneratorTestUtils.makeSequence( generator, SAMPLE_SIZE );
        double actualMean = GeneratorTestUtils.getSequenceMean( sequence );

        // Then
        String assertMessage = String.format( "Expected mean(%s) should equal actual mean(%s) within tolerance(%s)",
                expectedMean, actualMean, getMeanTolerance() );
        GeneratorTestUtils.assertWithinTolerance( assertMessage, expectedMean, actualMean, getMeanTolerance() );
    }

    @Test
    public void nextLastTest()
    {
        // Given
        Generator<? extends Number> generator = getGeneratorImpl();

        // When

        // Then
        GeneratorTestUtils.assertLastEqualsLastNext( generator, SAMPLE_SIZE );
    }

    private static class BucketComparator implements Comparator<Range<Double>>
    {
        public int compare( Range<Double> bucket1, Range<Double> bucket2 )
        {
            return bucket1.lowerEndpoint() < bucket2.lowerEndpoint() ? -1 : 1;
        }
    }
}
