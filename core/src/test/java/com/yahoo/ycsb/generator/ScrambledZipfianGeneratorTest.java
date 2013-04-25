package com.yahoo.ycsb.generator;

import java.util.Map;

import com.google.common.collect.Range;
import com.yahoo.ycsb.Histogram;

public class ScrambledZipfianGeneratorTest<T extends Number> extends NumberGeneratorTest<T>
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
    public Histogram<T> getExpectedDistribution()
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
