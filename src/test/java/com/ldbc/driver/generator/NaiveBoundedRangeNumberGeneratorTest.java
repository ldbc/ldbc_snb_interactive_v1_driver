package com.ldbc.driver.generator;

import com.ldbc.driver.util.Histogram;
import org.junit.Ignore;

@Ignore
public class NaiveBoundedRangeNumberGeneratorTest extends NumberGeneratorTest<Long, Long> {
    @Override
    public double getMeanTolerance() {
        return 0;
    }

    @Override
    public double getDistributionTolerance() {
        return 0;
    }

    @Override
    public Generator<Long> getGeneratorImpl(GeneratorFactory generatorFactory) {
        return null;
    }

    @Override
    public Histogram<Long, Long> getExpectedDistribution() {
        return null;
    }

    @Override
    public double getExpectedMean() {
        return 0;
    }

}
