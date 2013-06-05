package com.ldbc.driver.generator.ycsb;

import org.junit.Ignore;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.NumberGeneratorTest;
import com.ldbc.driver.util.Histogram;

@Ignore
public class YcsbFileGeneratorTest<T extends Number, C extends Number> extends NumberGeneratorTest<T, C>
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
