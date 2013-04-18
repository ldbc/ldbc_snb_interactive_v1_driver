package com.yahoo.ycsb.generator;

import java.util.Map;

import com.google.common.collect.Range;

public class ZipfianGeneratorTest extends NumberGeneratorTest
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
    public Generator<? extends Number> getGeneratorImpl()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<Range<Double>, Double> getExpectedDistribution()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Double getExpectedMean()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
