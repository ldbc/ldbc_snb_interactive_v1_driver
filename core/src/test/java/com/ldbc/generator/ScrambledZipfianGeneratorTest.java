package com.ldbc.generator;

import org.junit.Ignore;

import com.ldbc.generator.Generator;
import com.ldbc.util.Histogram;

@Ignore
public class ScrambledZipfianGeneratorTest<T extends Number, C extends Number> extends NumberGeneratorTest<T, C>
{

    @Override
    public double getMeanTolerance()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public double getDistributionTolerance()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Generator<T> getGeneratorImpl()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Histogram<T, C> getExpectedDistribution()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public double getExpectedMean()
    {
        // TODO Auto-generated method stub
        return 0;
    }

}
