package com.yahoo.ycsb.generator;

import java.lang.reflect.Array;
import java.util.Random;

import com.google.common.collect.Range;
import com.yahoo.ycsb.WorkloadException;

public class GeneratorFactory
{
    private final Random RANDOM;
    private final Long SEED;

    public GeneratorFactory( Random random )
    {
        RANDOM = random;
        SEED = null;
    }

    public GeneratorFactory( Long seed )
    {
        RANDOM = null;
        SEED = seed;
    }

    private Long getSeed()
    {
        return ( null == RANDOM ) ? SEED : RANDOM.nextLong();
    }

    private Random getRandom()
    {
        return new Random( getSeed() );
    }

    // TODO move to some Util class
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
        return new DiscreteGenerator( getRandom(), pairs );
    }

    // TODO test
    public Generator<Integer> buildConstantIntegerGenerator( int constant ) throws WorkloadException
    {
        return new ConstantIntegerGenerator( getRandom(), constant );
    }

    // TODO test
    public Generator<Integer> buildUniformIntegerGenerator( Range<Integer> range ) throws WorkloadException
    {
        return new UniformIntegerGenerator( getRandom(), range.lowerEndpoint(), range.upperEndpoint() );
    }

    // TODO test
    public Generator<Long> buildZipfianIntegerGenerator( Range<Integer> range ) throws WorkloadException
    {
        return new ZipfianGenerator( getRandom(), range.lowerEndpoint(), range.upperEndpoint() );
    }

    // TODO test
    public Generator<Integer> buildHistogramIntegerGenerator( String histogramFilePath ) throws GeneratorException
    {
        return new HistogramGenerator( getRandom(), histogramFilePath );
    }

}
