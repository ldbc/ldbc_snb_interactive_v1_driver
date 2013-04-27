package com.yahoo.ycsb;

import com.google.common.collect.Range;

public class RangeBucket extends Bucket<Number>
{
    private final Range<Double> range;

    public RangeBucket( Range<Double> range )
    {
        this.range = range;
    }

    @Override
    public boolean contains( Number number )
    {
        return range.contains( number.doubleValue() );
    }

    @Override
    public String toString()
    {
        return "RangeBucket [range=" + range + "]";
    }
}
