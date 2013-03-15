package com.yahoo.ycsb.generator;

import java.util.Iterator;

public interface RememberingIterator<T> extends Iterator<T>
{
    public T last();
}
