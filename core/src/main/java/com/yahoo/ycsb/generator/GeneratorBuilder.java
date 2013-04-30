package com.yahoo.ycsb.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.generator.ycsb.ExponentialGenerator;
import com.yahoo.ycsb.generator.ycsb.HotspotGenerator;
import com.yahoo.ycsb.generator.ycsb.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.ycsb.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.ycsb.ZipfianGenerator;
import com.yahoo.ycsb.util.Pair;
import com.yahoo.ycsb.util.RandomDataGeneratorFactory;

/**
 * Every returned Generator will use a different RandomDataGenerator
 */
public class GeneratorBuilder
{

    public static class GeneratorBuilderDelegate<G extends Generator<T>, T>
    {
        protected final Generator<T> generator;

        private GeneratorBuilderDelegate( Generator<T> generator )
        {
            this.generator = generator;
        }

        public Generator<T> build()
        {
            return generator;
        }
    }

    public static class NumberGeneratorBuilderDelegate<G extends Generator<T>, T extends Number> extends
            GeneratorBuilderDelegate<G, T>
    {
        private boolean withMinMaxLast = false;
        private T min = null;
        private T max = null;

        private NumberGeneratorBuilderDelegate( Generator<T> generator )
        {
            super( generator );
        }

        public GeneratorBuilderDelegate<G, T> withMinMaxLast( T min, T max )
        {
            this.withMinMaxLast = true;
            this.min = min;
            this.max = max;
            return this;
        }

        public Generator<T> build()
        {
            return ( true == withMinMaxLast ) ? new MinMaxGeneratorWrapper<T>( generator, min, max ) : generator;
        }
    }

    private final RandomDataGeneratorFactory randomDataGeneratorFactory;

    public GeneratorBuilder( RandomDataGeneratorFactory randomDataGeneratorFactory )
    {
        this.randomDataGeneratorFactory = randomDataGeneratorFactory;
    }

    private RandomDataGenerator getRandom()
    {
        return randomDataGeneratorFactory.newRandom();
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<UniformNumberGenerator<T>, T> newUniformNumberGenerator(
            T lowerBound, T upperBound )
    {
        UniformNumberGenerator<T> generator = new UniformNumberGenerator<T>( getRandom(), lowerBound, upperBound );
        return new NumberGeneratorBuilderDelegate<UniformNumberGenerator<T>, T>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<GrowingRangeUniformNumberGenerator<T>, T> newGrowingRangeUniformNumberGenerator(
            MinMaxGeneratorWrapper<T> boundingGenerator )
    {
        GrowingRangeUniformNumberGenerator<T> generator = new GrowingRangeUniformNumberGenerator<T>( getRandom(),
                boundingGenerator );
        return new NumberGeneratorBuilderDelegate<GrowingRangeUniformNumberGenerator<T>, T>( generator );
    }

    public <T> GeneratorBuilderDelegate<DiscreteGenerator<T>, T> newDiscreteGenerator( Iterable<Pair<Double, T>> pairs )
    {
        DiscreteGenerator<T> generator = new DiscreteGenerator<T>( getRandom(), pairs );
        return new GeneratorBuilderDelegate<DiscreteGenerator<T>, T>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<ConstantNumberGenerator<T>, T> newConstantIntegerGenerator(
            T constant )
    {
        ConstantNumberGenerator<T> generator = new ConstantNumberGenerator<T>( getRandom(), constant );
        return new NumberGeneratorBuilderDelegate<ConstantNumberGenerator<T>, T>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<CounterGenerator<T>, T> newCounterGenerator( T start,
            T incrementBy )
    {
        CounterGenerator<T> generator = new CounterGenerator<T>( getRandom(), start, incrementBy );
        return new NumberGeneratorBuilderDelegate<CounterGenerator<T>, T>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<ZipfianGenerator, Long> newZipfianGenerator( Long lowerBound, Long upperBound )
    {
        return newZipfianGenerator( lowerBound, upperBound, ZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<ZipfianGenerator, Long> newZipfianGenerator( Long lowerBound,
            Long upperBound, double zipfianConstant )
    {
        ZipfianGenerator generator = new ZipfianGenerator( getRandom(), lowerBound, upperBound, zipfianConstant );
        return new NumberGeneratorBuilderDelegate<ZipfianGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<ZipfianGenerator, Long> newZipfianGenerator( Long lowerBound,
            Long upperBound, double zipfianConstant, double zetan )
    {
        ZipfianGenerator generator = new ZipfianGenerator( getRandom(), lowerBound, upperBound, zipfianConstant, zetan );
        return new NumberGeneratorBuilderDelegate<ZipfianGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<ExponentialGenerator, Long> newExponentialGenerator( double percentile,
            double range )
    {
        ExponentialGenerator generator = new ExponentialGenerator( getRandom(), percentile, range );
        return new NumberGeneratorBuilderDelegate<ExponentialGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<ExponentialGenerator, Long> newExponentialGenerator( double mean )
    {
        ExponentialGenerator generator = new ExponentialGenerator( getRandom(), mean );
        return new NumberGeneratorBuilderDelegate<ExponentialGenerator, Long>( generator );
    }

    // TODO Generic
    // Create a zipfian generator for the specified number of items
    public NumberGeneratorBuilderDelegate<ScrambledZipfianGenerator, Long> newScrambledZipfianGenerator(
            Long lowerBound, Long upperBound )
    {
        return newScrambledZipfianGenerator( lowerBound, upperBound, ZipfianGenerator.ZIPFIAN_CONSTANT );
    }

    // TODO Generic
    /* 
     * Create a zipfian generator for items between min and max (inclusive) for
     * the specified zipfian constant. a zipfian constant other than 0.99 will take 
     * a long time to complete because we need to recompute zeta
     */
    public NumberGeneratorBuilderDelegate<ScrambledZipfianGenerator, Long> newScrambledZipfianGenerator(
            Long lowerBound, Long upperBound, double zipfianConstant )
    {
        ZipfianGenerator zipfianGenerator = null;
        if ( ZipfianGenerator.ZIPFIAN_CONSTANT == zipfianConstant )
        {
            zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( lowerBound, upperBound, zipfianConstant,
                    ScrambledZipfianGenerator.ZETAN ).build();
        }
        else
        {
            // Slower, has to recompute Zetan
            zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( lowerBound, upperBound, zipfianConstant ).build();
        }
        ScrambledZipfianGenerator generator = new ScrambledZipfianGenerator( getRandom(), lowerBound, upperBound,
                zipfianGenerator );
        return new NumberGeneratorBuilderDelegate<ScrambledZipfianGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<SkewedLatestGenerator, Long> newSkewedLatestGenerator(
            MinMaxGeneratorWrapper<Long> basis )
    {
        ZipfianGenerator zipfianGenerator = (ZipfianGenerator) newZipfianGenerator( 0l, basis.getMax() ).build();
        SkewedLatestGenerator generator = new SkewedLatestGenerator( getRandom(), basis, zipfianGenerator );
        return new NumberGeneratorBuilderDelegate<SkewedLatestGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<HotspotGenerator, Long> newHotspotGenerator( int lowerBound, int upperBound,
            double hotsetFraction, double hotOpnFraction )
    {
        HotspotGenerator generator = new HotspotGenerator( getRandom(), lowerBound, upperBound, hotsetFraction,
                hotOpnFraction );
        return new NumberGeneratorBuilderDelegate<HotspotGenerator, Long>( generator );
    }
}
