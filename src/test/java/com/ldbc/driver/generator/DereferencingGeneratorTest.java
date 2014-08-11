package com.ldbc.driver.generator;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.ldbc.driver.util.Bucket.NumberRangeBucket;
import com.ldbc.driver.util.Histogram;

import java.util.Iterator;

public class DereferencingGeneratorTest extends NumberGeneratorTest<Integer, Long> {
    @Override
    public double getMeanTolerance() {
        return 0.1;
    }

    @Override
    public double getDistributionTolerance() {
        return 0.01;
    }

    @Override
    public Iterator<Integer> getGeneratorImpl(GeneratorFactory generatorFactory) {
        Iterator<Integer> integerSequence1 = generatorFactory.constant(1);
        Iterator<Integer> integerSequence2 = generatorFactory.constant(2);
        Iterator<Integer> integerSequence3 = generatorFactory.constant(3);
        return generatorFactory.discreteDereferencing(Lists.newArrayList(integerSequence1, integerSequence2, integerSequence3));
    }

    @Override
    public Histogram<Integer, Long> getExpectedDistribution() {
        Histogram<Integer, Long> expectedDistribution = new Histogram<>(0l);
        expectedDistribution.addBucket(new NumberRangeBucket<Integer>(Range.closedOpen(-0.5d, 0.5d)), 0l);
        expectedDistribution.addBucket(new NumberRangeBucket<Integer>(Range.closedOpen(0.5d, 1.5d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Integer>(Range.closedOpen(1.5d, 2.5d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Integer>(Range.closedOpen(2.5d, 3.5d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Integer>(Range.closedOpen(3.5d, 4.5d)), 0l);
        expectedDistribution.addBucket(new NumberRangeBucket<Integer>(Range.closed(4.5d, 5.5d)), 0l);
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean() {
        return 1 + (((double) 3 - (double) 1) / 2);
    }
}
