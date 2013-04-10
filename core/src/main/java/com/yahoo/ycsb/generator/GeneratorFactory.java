package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.google.common.collect.Range;
import com.yahoo.ycsb.RandomDataGeneratorFactory;
import com.yahoo.ycsb.WorkloadException;

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

    public <T> DiscreteGenerator<T> newDiscreteGenerator( Pair<Double, T>... pairs )
    {
        return new DiscreteGenerator<T>( getRandom(), pairs );
    }

    // TODO test
    public ConstantNumberGenerator<Long> newConstantIntegerGenerator( long constant ) throws WorkloadException
    {
        return new ConstantNumberGenerator<Long>( getRandom(), constant );
    }

    // TODO test
    public Generator<Long> newUniformIntegerGenerator( Range<Long> range ) throws WorkloadException
    {
        return new UniformLongGenerator( getRandom(), range.lowerEndpoint(), range.upperEndpoint() );
    }

    // TODO test
    public Generator<Long> newZipfianGenerator( Range<Long> range ) throws WorkloadException
    {
        return newZipfianGenerator( range, ZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    // TODO test
    public Generator<Long> newZipfianGenerator( Range<Long> range, double zipfianConstant ) throws WorkloadException
    {
        return new ZipfianGenerator( getRandom(), range.lowerEndpoint(), range.upperEndpoint(), zipfianConstant );
    }

    // TODO test
    public Generator<Long> newZipfianGenerator( Range<Long> range, double zipfianConstant, double zetan )
            throws WorkloadException
    {
        return new ZipfianGenerator( getRandom(), range.lowerEndpoint(), range.upperEndpoint(), zipfianConstant, zetan );
    }

    // TODO test
    public Generator<Long> newHistogramIntegerGenerator( String histogramFilePath ) throws GeneratorException
    {
        return new HistogramGenerator( getRandom(), histogramFilePath );
    }

    // TODO test
    public Generator<Long> newExponentialGenerator( double percentile, double range )
    {
        return new ExponentialGenerator( getRandom(), percentile, range );
    }

    // TODO test
    public Generator<Long> newExponentialGenerator( double mean )
    {
        return new ExponentialGenerator( getRandom(), mean );
    }

    // TODO test
    public CounterGenerator newCounterGenerator( long start )
    {
        return new CounterGenerator( getRandom(), start );
    }

    // TODO test
    // Create a zipfian generator for the specified number of items
    public Generator<Long> newScrambledZipfianGenerator( Range<Long> range )
    {
        return newScrambledZipfianGenerator( range, ZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    // TODO test
    /* 
     * Create a zipfian generator for items between min and max (inclusive) for
     * the specified zipfian constant. a zipfian constant other than 0.99 will take 
     * a long time to complete because we need to recompute zeta
     */
    public Generator<Long> newScrambledZipfianGenerator( Range<Long> range, double zipfianConstant )
    {
        ZipfianGenerator zipfianGenerator = null;
        if ( ZipfianGenerator.ZIPFIAN_CONSTANT == zipfianConstant )
        {
            zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( range, zipfianConstant,
                    ScrambledZipfianLongGenerator.ZETAN );
        }
        else
        {
            // Slower, has to recompute Zetan
            zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( range, zipfianConstant );
        }
        return new ScrambledZipfianLongGenerator( getRandom(), range.lowerEndpoint(), range.upperEndpoint(),
                zipfianGenerator );
    }

    // TODO test
    public Generator<Long> newSkewedLatestGenerator( CounterGenerator basis )
    {
        ZipfianGenerator zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( Range.closed( 0l, basis.last() ) );
        return new SkewedLatestGenerator( getRandom(), basis, zipfianGenerator );
    }

    // TODO test
    public Generator<Long> newHotspotIntegerGenerator( int lowerBound, int upperBound, double hotsetFraction,
            double hotOpnFraction )
    {
        return new HotspotLongGenerator( getRandom(), lowerBound, upperBound, hotsetFraction, hotOpnFraction );
    }
}
