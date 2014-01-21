package com.ldbc.driver.generator;

import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IncrementingGeneratorTest extends NumberGeneratorTest<Long, Long> {
    private final long start = 0;

    @Override
    public double getMeanTolerance() {
        return 0.001;
    }

    @Override
    public double getDistributionTolerance() {
        return 0.001;
    }

    @Override
    public Iterator<Long> getGeneratorImpl(GeneratorFactory generatorFactory) {
        return getGeneratorFactory().incrementing(start, 1l);
    }

    @Override
    public Histogram<Long, Long> getExpectedDistribution() {
        Histogram<Long, Long> expectedDistribution = new Histogram<Long, Long>(0l);

        double min = (double) start;
        double max = (double) getSampleSize();
        int bucketCount = 10;
        List<Bucket<Long>> buckets = Histogram.makeBucketsOfEqualRange(min, max, bucketCount, Long.class);
        expectedDistribution.addBuckets(buckets, 1l);
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean() {
        return (getSampleSize() - 1) / 2.0;
    }

    @Test
    public void firstNumberShouldEqualStartTest() {
        // Given
        Iterator<Long> generator = getGeneratorFactory().incrementing(start, 1l);

        // When
        long firstNumber = generator.next();
        long secondNumber = generator.next();

        // Then
        assertEquals(start, firstNumber);
        assertEquals(start + 1, secondNumber);
    }

}
