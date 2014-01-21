package com.ldbc.driver.generator;

import com.google.common.collect.Range;
import com.ldbc.driver.util.Bucket.NumberRangeBucket;
import com.ldbc.driver.util.Histogram;

import java.util.Iterator;

public class DynamicRangeUniformNumberGeneratorTest extends NumberGeneratorTest<Long, Long> {
    private final long uniformMin = 50;
    private final long uniformMax = 60;
    private final long counterStart = 0;
    private final long counterIterations = 101;

    @Override
    public double getMeanTolerance() {
        return 0.1;
    }

    @Override
    public double getDistributionTolerance() {
        return 0.01;
    }

    @Override
    public Iterator<Long> getGeneratorImpl(GeneratorFactory generatorFactory) {
        MinMaxGenerator<Long> counterGenerator = generatorFactory.minMaxGenerator(
                generatorFactory.incrementing(counterStart, 1l), uniformMin, uniformMax);
        Iterator<Long> growingRangeUniformGenerator = generatorFactory.dynamicRangeUniform(
                counterGenerator);
        for (int i = 0; i < counterIterations; i++) {
            counterGenerator.next();
        }
        return growingRangeUniformGenerator;
    }

    @Override
    public Histogram<Long, Long> getExpectedDistribution() {
        Histogram<Long, Long> expectedDistribution = new Histogram<Long, Long>(0l);
        expectedDistribution.addBucket(new NumberRangeBucket<Long>(Range.closedOpen(0d, 20d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Long>(Range.closedOpen(20d, 40d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Long>(Range.closedOpen(40d, 60d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Long>(Range.closedOpen(60d, 80d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Long>(Range.closed(80d, 100d)), 1l);
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean() {
        return ((double) (counterIterations - 1) - (double) counterStart) / 2;
    }

}
