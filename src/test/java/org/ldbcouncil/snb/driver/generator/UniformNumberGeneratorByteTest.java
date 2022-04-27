package org.ldbcouncil.snb.driver.generator;

import com.google.common.collect.Range;
import org.ldbcouncil.snb.driver.util.Bucket.NumberRangeBucket;
import org.ldbcouncil.snb.driver.util.Histogram;

import java.util.Iterator;

public class UniformNumberGeneratorByteTest extends NumberGeneratorTest<Byte, Long> {
    @Override
    public double getMeanTolerance() {
        return 1;
    }

    @Override
    public double getDistributionTolerance() {
        return 0.05;
    }

    @Override
    public Iterator<Byte> getGeneratorImpl(GeneratorFactory generatorFactory) {
        return generatorFactory.uniformBytes();
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
