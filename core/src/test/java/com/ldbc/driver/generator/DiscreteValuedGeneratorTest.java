package com.ldbc.driver.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Ignore;
import org.junit.Test;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.Pair;
import com.ldbc.driver.util.Bucket.DiscreteBucket;

@Ignore
public class DiscreteValuedGeneratorTest extends GeneratorTest<String, Integer>
{

    @Override
    public Histogram<String, Integer> getExpectedDistribution()
    {
        return null;
    }

    @Override
    public double getDistributionTolerance()
    {
        return 0.00;
    }

    @Override
    public Generator<String> getGeneratorImpl()
    {
        return null;
    }
}
