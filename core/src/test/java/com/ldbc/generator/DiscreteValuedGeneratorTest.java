package com.ldbc.generator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorException;
import com.ldbc.util.Histogram;
import com.ldbc.util.Pair;
import com.ldbc.util.Bucket.DiscreteBucket;

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
