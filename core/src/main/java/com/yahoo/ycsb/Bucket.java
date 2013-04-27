package com.yahoo.ycsb;

public abstract class Bucket<T>
{
    public abstract boolean contains( T thing );
}
