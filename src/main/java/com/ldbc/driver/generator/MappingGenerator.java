package com.ldbc.driver.generator;

import com.ldbc.driver.util.Function1;

import java.util.Iterator;

public class MappingGenerator<FROM_GENERATE_TYPE, TO_GENERATE_TYPE> extends Generator<TO_GENERATE_TYPE> {
    private final Iterator<FROM_GENERATE_TYPE> fromGenerator;
    private final Function1<FROM_GENERATE_TYPE, TO_GENERATE_TYPE> mapFunction;

    public MappingGenerator(Iterator<FROM_GENERATE_TYPE> fromGenerator,
                            Function1<FROM_GENERATE_TYPE, TO_GENERATE_TYPE> mapFunction) {
        this.fromGenerator = fromGenerator;
        this.mapFunction = mapFunction;
    }

    @Override
    protected TO_GENERATE_TYPE doNext() throws GeneratorException {
        return (fromGenerator.hasNext()) ? mapFunction.apply(fromGenerator.next()) : null;
    }
}
