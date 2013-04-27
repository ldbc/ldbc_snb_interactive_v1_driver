package com.yahoo.ycsb;

public class DiscreteBucket<T extends Object> extends Bucket<T>
{
    private final T thing;

    public DiscreteBucket( T thing )
    {
        this.thing = thing;
    }

    @Override
    public boolean contains( T thing )
    {
        return this.thing.equals( thing );
    }

    @Override
    public String toString()
    {
        return "DiscreteBucket [thing=" + thing + "]";
    }
}
