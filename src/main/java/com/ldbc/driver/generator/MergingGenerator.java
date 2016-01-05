package com.ldbc.driver.generator;

import com.ldbc.driver.util.Function2;

import java.util.Iterator;

// TODO test
public class MergingGenerator<FROM_GENERATE_TYPE_1, FROM_GENERATE_TYPE_2, TO_GENERATE_TYPE>
        extends Generator<TO_GENERATE_TYPE>
{
    private final Iterator<FROM_GENERATE_TYPE_1> original1;
    private final Iterator<FROM_GENERATE_TYPE_2> original2;
    private final Function2<FROM_GENERATE_TYPE_1,FROM_GENERATE_TYPE_2,TO_GENERATE_TYPE,RuntimeException> mergeFun;

    MergingGenerator(
            Iterator<FROM_GENERATE_TYPE_1> original1,
            Iterator<FROM_GENERATE_TYPE_2> original2,
            Function2<FROM_GENERATE_TYPE_1,FROM_GENERATE_TYPE_2,TO_GENERATE_TYPE,RuntimeException> mergeFun )
    {
        this.original1 = original1;
        this.original2 = original2;
        this.mergeFun = mergeFun;
    }

    @Override
    protected TO_GENERATE_TYPE doNext() throws GeneratorException
    {
        if ( original1.hasNext() && original2.hasNext() )
        { return mergeFun.apply( original1.next(), original2.next() ); }
        else
        { return null; }
    }
}
