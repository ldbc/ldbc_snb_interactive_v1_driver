package com.ldbc.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.Iterator;

public class UniformByteIteratorGenerator extends Generator<Iterator<Byte>> {
    private final Iterator<Long> lengths;
    private final RandomDataGenerator random;
    private final GeneratorFactory gf;

    UniformByteIteratorGenerator(RandomDataGenerator random, Iterator<Long> lengths, GeneratorFactory gf) {
        this.random = random;
        this.lengths = lengths;
        this.gf = gf;
    }

    @Override
    protected Iterator<Byte> doNext() throws GeneratorException {
        return gf.limit(gf.uniformBytes(), lengths.next());
    }

}
