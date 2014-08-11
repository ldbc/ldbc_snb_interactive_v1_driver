package com.ldbc.driver.generator;

import java.util.Iterator;

public class SizedUniformByteGeneratorGenerator extends Generator<Iterator<Byte>> {
    private final Iterator<Long> lengths;
    private final GeneratorFactory gf;

    SizedUniformByteGeneratorGenerator(Iterator<Long> lengths, GeneratorFactory gf) {
        this.lengths = lengths;
        this.gf = gf;
    }

    @Override
    protected Iterator<Byte> doNext() throws GeneratorException {
        return gf.limit(gf.uniformBytes(), lengths.next());
    }

}
