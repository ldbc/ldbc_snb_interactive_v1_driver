package com.ldbc.driver.generator;

import com.ldbc.driver.util.Histogram;
import org.junit.Ignore;

@Ignore
public class DereferencingGeneratorTest extends GeneratorTest<String, Integer> {

    @Override
    public Histogram<String, Integer> getExpectedDistribution() {
        return null;
    }

    @Override
    public double getDistributionTolerance() {
        return 0.00;
    }

    @Override
    public Generator<String> getGeneratorImpl(GeneratorFactory generatorFactory) {
        return null;
    }
}
