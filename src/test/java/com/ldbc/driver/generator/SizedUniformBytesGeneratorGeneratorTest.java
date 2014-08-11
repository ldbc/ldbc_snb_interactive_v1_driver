package com.ldbc.driver.generator;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.ldbc.driver.util.Bucket.NumberRangeBucket;
import com.ldbc.driver.util.Histogram;

import java.util.Iterator;

public class SizedUniformBytesGeneratorGeneratorTest extends NumberGeneratorTest<Byte, Long> {
    @Override
    public double getMeanTolerance() {
        return 1;
    }

    @Override
    public double getDistributionTolerance() {
        return 0.05;
    }

    @Override
    public Iterator<Byte> getGeneratorImpl(GeneratorFactory gf) {
        Iterator<Iterator<Byte>> uniformBytesGeneratorGenerator =
                gf.limit(
                        // generate uniform byte generators of maximum length
                        gf.sizedUniformBytesGenerator(
                                gf.constant(Long.MAX_VALUE)
                        ),
                        // generate 100 generators
                        100
                );
        // create dereferencing generator that will pull the bytes from the 100 byte generators
        return gf.discreteDereferencing(Lists.newArrayList(uniformBytesGeneratorGenerator));
    }

    @Override
    public Histogram<Byte, Long> getExpectedDistribution() {
        Histogram<Byte, Long> expectedDistribution = new Histogram<>(0l);
        expectedDistribution.addBucket(new NumberRangeBucket<Byte>(Range.closedOpen(-128d, -64d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Byte>(Range.closedOpen(-64d, 0d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Byte>(Range.closedOpen(0d, 64d)), 1l);
        expectedDistribution.addBucket(new NumberRangeBucket<Byte>(Range.closed(64d, 128d)), 1l);
        return expectedDistribution;
    }

    @Override
    public double getExpectedMean() {
        return 0d;
    }
}
