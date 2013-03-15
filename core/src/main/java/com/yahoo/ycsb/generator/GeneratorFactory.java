package com.yahoo.ycsb.generator;

import java.lang.reflect.Array;
import com.google.common.collect.Range;
import com.yahoo.ycsb.WorkloadException;

public class GeneratorFactory
{

    // TODO move all Generators to .graph workpackage and do them properly

    // TODO create MeanableIntegerGenerator & MeanableLongGenerator?
    // TODO or MeanableGenerator<T>?
    // TODO make all Generator<Long> by default?

    public GeneratorFactory()
    {
    }

    public DiscreteGenerator convertToDiscreteGenerator( Object... pairs )
    {
        Pair<Double, Object>[] pairsArray = (Pair<Double, Object>[]) Array.newInstance( Pair.class, pairs.length / 2 );
        int i = 0;
        while ( i < pairs.length )
        {
            int propIndex = i++;
            int opIndex = i++;
            Pair<Double, Object> pair = new Pair<Double, Object>( (Double) pairs[propIndex], pairs[opIndex] );
            pairsArray[( i / 2 ) - 1] = pair;
        }
        return buildDiscreteGenerator( pairsArray );
    }

    public DiscreteGenerator buildDiscreteGenerator( Pair<Double, Object>... pairs )
    {
        return new DiscreteGenerator( pairs );
    }

    // TODO test
    public Generator<Integer> buildConstantIntegerGenerator( int constant ) throws WorkloadException
    {
        return new ConstantIntegerGenerator( constant );
    }

    // TODO test
    public Generator<Integer> buildUniformIntegerGenerator( Range<Integer> range ) throws WorkloadException
    {
        return new UniformIntegerGenerator( range.lowerEndpoint(), range.upperEndpoint() );
    }

    // TODO test
    public Generator<Long> buildZipfianIntegerGenerator( Range<Integer> range ) throws WorkloadException
    {
        return new ZipfianGenerator( range.lowerEndpoint(), range.upperEndpoint() );
    }

    // TODO test
    public Generator<Integer> buildHistogramIntegerGenerator( String histogramFilePath ) throws GeneratorException
    {
        return new HistogramGenerator( histogramFilePath );
    }

}
