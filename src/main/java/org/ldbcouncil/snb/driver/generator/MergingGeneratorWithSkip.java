package org.ldbcouncil.snb.driver.generator;

import org.ldbcouncil.snb.driver.util.Function2;

import java.util.Iterator;

/**
 * Merging Generator which skips null results for the second FROM_GENERATE_TYPE_2. Find
 * suitable FROM_GENERATE_TYPE_1 and FROM_GENERATE_TYPE_2 combinations.
 */
public class MergingGeneratorWithSkip<FROM_GENERATE_TYPE_1, FROM_GENERATE_TYPE_2, TO_GENERATE_TYPE>
        extends Generator<TO_GENERATE_TYPE>
{
    private final int MAX_SKIP = 100;
    private final Iterator<FROM_GENERATE_TYPE_1> original1;
    private final Iterator<FROM_GENERATE_TYPE_2> original2;
    private final Function2<FROM_GENERATE_TYPE_1,FROM_GENERATE_TYPE_2,TO_GENERATE_TYPE,RuntimeException> mergeFun;
    private final Function2<FROM_GENERATE_TYPE_1,FROM_GENERATE_TYPE_2,TO_GENERATE_TYPE,RuntimeException> checkFun;

    public MergingGeneratorWithSkip(
            Iterator<FROM_GENERATE_TYPE_1> original1,
            Iterator<FROM_GENERATE_TYPE_2> original2,
            Function2<FROM_GENERATE_TYPE_1,FROM_GENERATE_TYPE_2,TO_GENERATE_TYPE,RuntimeException> mergeFun,
            Function2<FROM_GENERATE_TYPE_1, FROM_GENERATE_TYPE_2, TO_GENERATE_TYPE, RuntimeException> checkFun)
    {
        this.original1 = original1;
        this.original2 = original2;
        this.mergeFun = mergeFun;
        this.checkFun = checkFun;
    }

    @Override
    protected TO_GENERATE_TYPE doNext() throws GeneratorException
    {
        TO_GENERATE_TYPE operation = null;
        FROM_GENERATE_TYPE_1 type1;
        FROM_GENERATE_TYPE_2 type2;
        int skipped = 0;
        if ( original1.hasNext() && original2.hasNext() )
        {
            type1 = original1.next();
            type2 = original2.next();
        }
        else
        {
            return null;
        }

        while (original2.hasNext() && operation == null && skipped < MAX_SKIP)
        {
            operation = checkFun.apply( type1, type2 );
            type2 = original2.next();
            skipped++;
        }

        if (skipped > MAX_SKIP)
        {
            return null;
        }

        operation = mergeFun.apply( type1, type2 );
        return operation;
    }
}
