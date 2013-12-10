package com.ldbc.driver.generator;

import java.util.Iterator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.data.RandomByteIterator;

// TODO somehow generalize to all ByteIterators?
public class RandomByteIteratorGenerator extends Generator<ByteIterator>
{
    private final Iterator<Integer> lengthGenerator;
    private final RandomDataGenerator random;

    protected RandomByteIteratorGenerator( RandomDataGenerator random, Iterator<Integer> lengthGenerator )
    {
        this.random = random;
        this.lengthGenerator = lengthGenerator;
    }

    @Override
    protected ByteIterator doNext() throws GeneratorException
    {
        return new RandomByteIterator( lengthGenerator.next(), random );
    }

}
