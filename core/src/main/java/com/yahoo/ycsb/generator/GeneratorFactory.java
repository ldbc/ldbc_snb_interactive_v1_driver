package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.Pair;
import com.yahoo.ycsb.RandomDataGeneratorFactory;

/**
 * Every returned Generator will use a different RandomDataGenerator
 */
public class GeneratorFactory
{
    private final RandomDataGeneratorFactory randomDataGeneratorFactory;

    public GeneratorFactory( RandomDataGeneratorFactory randomDataGeneratorFactory )
    {
        this.randomDataGeneratorFactory = randomDataGeneratorFactory;
    }

    private RandomDataGenerator getRandom()
    {
        return randomDataGeneratorFactory.newRandom();
    }

    public <T extends Number> UniformNumberGenerator<T> newUniformNumberGenerator( T lowerBound, T upperBound )
    {
        return new UniformNumberGenerator<T>( getRandom(), lowerBound, upperBound );
    }

    public <T> DiscreteGenerator<T> newDiscreteGenerator( Pair<Double, T>... pairs )
    {
        return new DiscreteGenerator<T>( getRandom(), pairs );
    }

    // TODO test
    // TODO Generic
    public ConstantNumberGenerator<Long> newConstantIntegerGenerator( long constant )
    {
        return new ConstantNumberGenerator<Long>( getRandom(), constant );
    }

    // TODO test
    // TODO Generic
    public ZipfianGenerator newZipfianGenerator( Long lowerBound, Long upperBound )
    {
        return newZipfianGenerator( lowerBound, upperBound, ZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    // TODO test
    // TODO Generic
    public ZipfianGenerator newZipfianGenerator( Long lowerBound, Long upperBound, double zipfianConstant )
    {
        return new ZipfianGenerator( getRandom(), lowerBound, upperBound, zipfianConstant );
    }

    // TODO test
    // TODO Generic
    public ZipfianGenerator newZipfianGenerator( Long lowerBound, Long upperBound, double zipfianConstant, double zetan )
    {
        return new ZipfianGenerator( getRandom(), lowerBound, upperBound, zipfianConstant, zetan );
    }

    // TODO test
    // TODO Generic
    public HistogramGenerator newHistogramIntegerGenerator( String histogramFilePath ) throws GeneratorException
    {
        return new HistogramGenerator( getRandom(), histogramFilePath );
    }

    // TODO test
    // TODO Generic
    public ExponentialGenerator newExponentialGenerator( double percentile, double range )
    {
        return new ExponentialGenerator( getRandom(), percentile, range );
    }

    // TODO test
    // TODO Generic
    public ExponentialGenerator newExponentialGenerator( double mean )
    {
        return new ExponentialGenerator( getRandom(), mean );
    }

    // TODO test
    // TODO Generic
    public CounterGenerator newCounterGenerator( long start )
    {
        return new CounterGenerator( getRandom(), start );
    }

    // TODO test
    // TODO Generic
    // Create a zipfian generator for the specified number of items
    public ScrambledZipfianGenerator newScrambledZipfianGenerator( Long lowerBound, Long upperBound )
    {
        return newScrambledZipfianGenerator( lowerBound, upperBound, ZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    // TODO test
    // TODO Generic
    /* 
     * Create a zipfian generator for items between min and max (inclusive) for
     * the specified zipfian constant. a zipfian constant other than 0.99 will take 
     * a long time to complete because we need to recompute zeta
     */
    public ScrambledZipfianGenerator newScrambledZipfianGenerator( Long lowerBound, Long upperBound,
            double zipfianConstant )
    {
        ZipfianGenerator zipfianGenerator = null;
        if ( ZipfianGenerator.ZIPFIAN_CONSTANT == zipfianConstant )
        {
            zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( lowerBound, upperBound, zipfianConstant,
                    ScrambledZipfianGenerator.ZETAN );
        }
        else
        {
            // Slower, has to recompute Zetan
            zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( lowerBound, upperBound, zipfianConstant );
        }
        return new ScrambledZipfianGenerator( getRandom(), lowerBound, upperBound, zipfianGenerator );
    }

    // TODO test
    // TODO Generic
    public SkewedLatestGenerator newSkewedLatestGenerator( CounterGenerator basis )
    {
        ZipfianGenerator zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( 0l, basis.last() );
        return new SkewedLatestGenerator( getRandom(), basis, zipfianGenerator );
    }

    // TODO test
    // TODO Generic
    public HotspotGenerator newHotspotGenerator( int lowerBound, int upperBound, double hotsetFraction,
            double hotOpnFraction )
    {
        return new HotspotGenerator( getRandom(), lowerBound, upperBound, hotsetFraction, hotOpnFraction );
    }
}
