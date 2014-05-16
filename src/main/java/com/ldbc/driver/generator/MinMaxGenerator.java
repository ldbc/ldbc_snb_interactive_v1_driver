package com.ldbc.driver.generator;

import java.util.Iterator;


public class MinMaxGenerator<GENERATE_TYPE extends Number> extends Generator<GENERATE_TYPE>
{
    private GENERATE_TYPE min = null;
    private GENERATE_TYPE max = null;
    private final Iterator<GENERATE_TYPE> generator;

    MinMaxGenerator( Iterator<GENERATE_TYPE> generator, GENERATE_TYPE initialMin, GENERATE_TYPE initialMax )
    {
        this.min = initialMin;
        this.max = initialMax;
        this.generator = generator;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        if ( false == generator.hasNext() ) return null;
        GENERATE_TYPE next = generator.next();
        min = ( next.doubleValue() < min.doubleValue() ) ? next : min;
        max = ( next.doubleValue() > max.doubleValue() ) ? next : max;
        return next;

    }

    public final GENERATE_TYPE getMin()
    {
        return min;
    }

    public final GENERATE_TYPE getMax()
    {
        return max;
    }

    @Override
    public String toString()
    {
        return "MinMaxGeneratorWrapper [min=" + min + ", max=" + max + ", generator=" + generator + "]";
    }
}
