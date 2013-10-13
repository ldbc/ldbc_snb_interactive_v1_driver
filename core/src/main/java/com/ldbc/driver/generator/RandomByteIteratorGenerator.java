package com.ldbc.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.data.RandomByteIterator;

// TODO somehow generalize to all ByteIterators?
public class RandomByteIteratorGenerator extends Generator<ByteIterator>
{
    private final Generator<Integer> lengthGenerator;
    private final RandomDataGenerator random;

    protected RandomByteIteratorGenerator( RandomDataGenerator random, Generator<Integer> lengthGenerator )
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
