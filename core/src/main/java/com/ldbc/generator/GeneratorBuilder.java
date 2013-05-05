package com.ldbc.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.generator.ycsb.YcsbExponentialGenerator;
import com.ldbc.generator.ycsb.YcsbHotspotGenerator;
import com.ldbc.generator.ycsb.YcsbScrambledZipfianGenerator;
import com.ldbc.generator.ycsb.YcsbSkewedLatestGenerator;
import com.ldbc.generator.ycsb.YcsbZipfianGenerator;
import com.ldbc.generator.ycsb.YcsbZipfianNumberGenerator;
import com.ldbc.util.Pair;
import com.ldbc.util.RandomDataGeneratorFactory;

public class GeneratorBuilder
{

    public static class GeneratorBuilderDelegate<G extends Generator<?>, T>
    {
        protected final G generator;

        private GeneratorBuilderDelegate( G generator )
        {
            this.generator = generator;
        }

        public final G build()
        {
            return generator;
        }
    }

    public static class NumberGeneratorBuilderDelegate<G extends Generator<T>, T extends Number> extends
            GeneratorBuilderDelegate<G, T>
    {
        private NumberGeneratorBuilderDelegate( G generator )
        {
            super( generator );
        }

        public GeneratorBuilderDelegate<MinMaxGeneratorWrapper<T>, T> withMinMaxLast( T min, T max )
        {
            MinMaxGeneratorWrapper<T> minMaxGenerator = new MinMaxGeneratorWrapper<T>( generator, min, max );
            return new GeneratorBuilderDelegate<MinMaxGeneratorWrapper<T>, T>( minMaxGenerator );
        }
    }

    private final RandomDataGeneratorFactory randomDataGeneratorFactory;

    public GeneratorBuilder( RandomDataGeneratorFactory randomDataGeneratorFactory )
    {
        this.randomDataGeneratorFactory = randomDataGeneratorFactory;
    }

    // Every returned Generator will use a different RandomDataGenerator
    private RandomDataGenerator getRandom()
    {
        return randomDataGeneratorFactory.newRandom();
    }

    public <T> GeneratorBuilderDelegate<DiscreteGenerator<T>, T> discreteGenerator( Iterable<Pair<Double, T>> pairs )
    {
        DiscreteGenerator<T> generator = new DiscreteGenerator<T>( getRandom(), pairs );
        return new GeneratorBuilderDelegate<DiscreteGenerator<T>, T>( generator );
    }

    public <T> GeneratorBuilderDelegate<DiscreteMultiGenerator<T>, T> discreteMultiGenerator(
            Iterable<Pair<Double, T>> pairs, Integer amountToRetrieve )
    {
        Generator<Integer> amountToRetrieveGenerator = constantGenerator( amountToRetrieve ).build();
        DiscreteMultiGenerator<T> generator = new DiscreteMultiGenerator<T>( getRandom(), pairs,
                amountToRetrieveGenerator );
        return new GeneratorBuilderDelegate<DiscreteMultiGenerator<T>, T>( generator );
    }

    public <T> GeneratorBuilderDelegate<DiscreteMultiGenerator<T>, T> discreteMultiGenerator(
            Iterable<Pair<Double, T>> pairs, Generator<Integer> amountToRetrieveGenerator )
    {
        DiscreteMultiGenerator<T> generator = new DiscreteMultiGenerator<T>( getRandom(), pairs,
                amountToRetrieveGenerator );
        return new GeneratorBuilderDelegate<DiscreteMultiGenerator<T>, T>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<UniformNumberGenerator<T>, T> uniformNumberGenerator(
            T lowerBound, T upperBound )
    {
        UniformNumberGenerator<T> generator = new UniformNumberGenerator<T>( getRandom(), lowerBound, upperBound );
        return new NumberGeneratorBuilderDelegate<UniformNumberGenerator<T>, T>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<GrowingRangeUniformNumberGenerator<T>, T> growingRangeUniformNumberGenerator(
            MinMaxGeneratorWrapper<T> boundingGenerator )
    {
        GrowingRangeUniformNumberGenerator<T> generator = new GrowingRangeUniformNumberGenerator<T>( getRandom(),
                boundingGenerator );
        return new NumberGeneratorBuilderDelegate<GrowingRangeUniformNumberGenerator<T>, T>( generator );
    }

    public <T> GeneratorBuilderDelegate<ConstantGenerator<T>, T> constantGenerator( T constant )
    {
        ConstantGenerator<T> generator = new ConstantGenerator<T>( getRandom(), constant );
        return new GeneratorBuilderDelegate<ConstantGenerator<T>, T>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<CounterGenerator<T>, T> counterGenerator( T start,
            T incrementBy )
    {
        CounterGenerator<T> generator = new CounterGenerator<T>( getRandom(), start, incrementBy );
        return new NumberGeneratorBuilderDelegate<CounterGenerator<T>, T>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<YcsbZipfianNumberGenerator<T>, T> zipfianNumberGenerator(
            T lowerBound, T upperBound )
    {
        return zipfianNumberGenerator( lowerBound, upperBound, YcsbZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<YcsbZipfianNumberGenerator<T>, T> zipfianNumberGenerator(
            T lowerBound, T upperBound, double zipfianConstant )
    {
        YcsbZipfianNumberGenerator<T> generator = new YcsbZipfianNumberGenerator<T>( getRandom(), lowerBound,
                upperBound, zipfianConstant );
        return new NumberGeneratorBuilderDelegate<YcsbZipfianNumberGenerator<T>, T>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<YcsbZipfianNumberGenerator<T>, T> zipfianNumberGenerator(
            T lowerBound, T upperBound, double zipfianConstant, double zetan )
    {
        YcsbZipfianNumberGenerator<T> generator = new YcsbZipfianNumberGenerator<T>( getRandom(), lowerBound,
                upperBound, zipfianConstant, zetan );
        return new NumberGeneratorBuilderDelegate<YcsbZipfianNumberGenerator<T>, T>( generator );
    }

    // TODO Generic
    @Deprecated
    public NumberGeneratorBuilderDelegate<YcsbZipfianGenerator, Long> zipfianGenerator( Long lowerBound, Long upperBound )
    {
        return zipfianGenerator( lowerBound, upperBound, YcsbZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    // TODO Generic
    @Deprecated
    public NumberGeneratorBuilderDelegate<YcsbZipfianGenerator, Long> zipfianGenerator( Long lowerBound,
            Long upperBound, double zipfianConstant )
    {
        YcsbZipfianGenerator generator = new YcsbZipfianGenerator( getRandom(), lowerBound, upperBound, zipfianConstant );
        return new NumberGeneratorBuilderDelegate<YcsbZipfianGenerator, Long>( generator );
    }

    // TODO Generic
    @Deprecated
    public NumberGeneratorBuilderDelegate<YcsbZipfianGenerator, Long> zipfianGenerator( Long lowerBound,
            Long upperBound, double zipfianConstant, double zetan )
    {
        YcsbZipfianGenerator generator = new YcsbZipfianGenerator( getRandom(), lowerBound, upperBound,
                zipfianConstant, zetan );
        return new NumberGeneratorBuilderDelegate<YcsbZipfianGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<YcsbExponentialGenerator, Long> exponentialGenerator( double percentile,
            double range )
    {
        YcsbExponentialGenerator generator = new YcsbExponentialGenerator( getRandom(), percentile, range );
        return new NumberGeneratorBuilderDelegate<YcsbExponentialGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<YcsbExponentialGenerator, Long> exponentialGenerator( double mean )
    {
        YcsbExponentialGenerator generator = new YcsbExponentialGenerator( getRandom(), mean );
        return new NumberGeneratorBuilderDelegate<YcsbExponentialGenerator, Long>( generator );
    }

    // TODO Generic
    // Create a zipfian generator for the specified number of items
    public NumberGeneratorBuilderDelegate<YcsbScrambledZipfianGenerator, Long> scrambledZipfianGenerator(
            Long lowerBound, Long upperBound )
    {
        return scrambledZipfianGenerator( lowerBound, upperBound, YcsbZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    // TODO Generic
    /* 
     * Create a zipfian generator for items between min and max (inclusive) for
     * the specified zipfian constant. a zipfian constant other than 0.99 will take 
     * a long time to complete because we need to recompute zeta
     */
    public NumberGeneratorBuilderDelegate<YcsbScrambledZipfianGenerator, Long> scrambledZipfianGenerator(
            Long lowerBound, Long upperBound, double zipfianConstant )
    {
        YcsbZipfianGenerator zipfianGenerator = null;
        if ( YcsbZipfianGenerator.ZIPFIAN_CONSTANT == zipfianConstant )
        {
            zipfianGenerator = (YcsbZipfianGenerator) zipfianGenerator( lowerBound, upperBound, zipfianConstant,
                    YcsbScrambledZipfianGenerator.ZETAN ).build();
        }
        else
        {
            // Slower, has to recompute Zetan
            zipfianGenerator = (YcsbZipfianGenerator) zipfianGenerator( lowerBound, upperBound, zipfianConstant ).build();
        }
        YcsbScrambledZipfianGenerator generator = new YcsbScrambledZipfianGenerator( getRandom(), lowerBound,
                upperBound, zipfianGenerator );
        return new NumberGeneratorBuilderDelegate<YcsbScrambledZipfianGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<YcsbSkewedLatestGenerator, Long> skewedLatestGenerator(
            MinMaxGeneratorWrapper<Long> maxGenerator )
    {
        YcsbZipfianGenerator zipfianGenerator = (YcsbZipfianGenerator) zipfianGenerator( 0l, maxGenerator.getMax() ).build();
        YcsbSkewedLatestGenerator generator = new YcsbSkewedLatestGenerator( getRandom(), maxGenerator,
                zipfianGenerator );
        return new NumberGeneratorBuilderDelegate<YcsbSkewedLatestGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<YcsbHotspotGenerator, Long> hotspotGenerator( long lowerBound,
            long upperBound, double hotsetFraction, double hotOpnFraction )
    {
        YcsbHotspotGenerator generator = new YcsbHotspotGenerator( getRandom(), lowerBound, upperBound, hotsetFraction,
                hotOpnFraction );
        return new NumberGeneratorBuilderDelegate<YcsbHotspotGenerator, Long>( generator );
    }
}
