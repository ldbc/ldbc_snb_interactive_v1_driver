package com.ldbc.driver.generator;

import com.google.common.collect.Range;
import com.ldbc.driver.util.Bucket.NumberRangeBucket;
import com.ldbc.driver.util.Histogram;

import java.util.Iterator;

public class ExponentialNumberGeneratorTest extends NumberGeneratorTest<Double, Integer> {
    private final Double mean = 43.43;

    @Override
    public double getMeanTolerance() {
        // TODO is this tolerance too generous?
        return 0.1;
    }

    @Override
    public double getDistributionTolerance() {
        // TODO is this tolerance too generous?
        return 0.03;
    }

    @Override
    public Iterator<Double> getGeneratorImpl(GeneratorFactory generatorFactory) {
        return generatorFactory.exponential(mean);
    }

    @Override
    public Histogram<Double, Integer> getExpectedDistribution() {
        Histogram<Double, Integer> expectedDistribution = new Histogram<Double, Integer>(0);
        expectedDistribution.addBucket(new NumberRangeBucket<Double>(Range.closedOpen(0d, 90d)), 9);
        expectedDistribution.addBucket(new NumberRangeBucket<Double>(Range.closed(90d, Double.MAX_VALUE)), 1);
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean() {
        return mean;
    }
}
