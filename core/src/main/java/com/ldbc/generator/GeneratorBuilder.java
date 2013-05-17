package com.ldbc.generator;

import java.util.ArrayList;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.generator.ycsb.YcsbDynamicRangeHotspotGenerator;
import com.ldbc.generator.ycsb.YcsbScrambledZipfianGenerator;
import com.ldbc.generator.ycsb.YcsbSkewedLatestGenerator;
import com.ldbc.generator.ycsb.YcsbZipfianNumberGenerator;
import com.ldbc.util.Pair;
import com.ldbc.util.RandomDataGeneratorFactory;
import com.ldbc.util.Triple;

public class GeneratorBuilder
{

    public static class GeneratorBuilderDelegate<G extends Generator<?>>
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
            GeneratorBuilderDelegate<G>
    {
        private NumberGeneratorBuilderDelegate( G generator )
        {
            super( generator );
        }

        public final GeneratorBuilderDelegate<PrefixGeneratorWrapper> withPrefix( String prefix )
        {
            PrefixGeneratorWrapper prefixGenerator = new PrefixGeneratorWrapper( generator, prefix );
            return new GeneratorBuilderDelegate<PrefixGeneratorWrapper>( prefixGenerator );
        }

        public GeneratorBuilderDelegate<MinMaxGeneratorWrapper<T>> withMinMax( T min, T max )
        {
            MinMaxGeneratorWrapper<T> minMaxGenerator = new MinMaxGeneratorWrapper<T>( generator, min, max );
            return new GeneratorBuilderDelegate<MinMaxGeneratorWrapper<T>>( minMaxGenerator );
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

    // TODO rename/design Discrete* generators, "multi" & "valued" is confusing
    // TODO "valued" -> "de(ref?)", "multi" is good, introduce "pair"

    /**
     * PrefixGeneratorWrapper
     */
    public <T extends Number> GeneratorBuilderDelegate<PrefixGeneratorWrapper> prefixGeneratorWrapper(
            Generator<T> generator, String prefix )
    {
        PrefixGeneratorWrapper generatorWrapper = new PrefixGeneratorWrapper( generator, prefix );
        return new GeneratorBuilderDelegate<PrefixGeneratorWrapper>( generatorWrapper );
    }

    /**
     * MinMaxGeneratorWrapper
     */
    public <T extends Number> GeneratorBuilderDelegate<MinMaxGeneratorWrapper<T>> minMaxGeneratorWrapper(
            Generator<T> generator, T initialMin, T initialMax )
    {
        MinMaxGeneratorWrapper<T> generatorWrapper = new MinMaxGeneratorWrapper<T>( generator, initialMin, initialMax );
        return new GeneratorBuilderDelegate<MinMaxGeneratorWrapper<T>>( generatorWrapper );
    }

    /**
     * DiscreteGenerator
     */
    public <T> GeneratorBuilderDelegate<DiscreteGenerator<T>> discreteGenerator( Iterable<Pair<Double, T>> items )
    {
        DiscreteGenerator<T> generator = new DiscreteGenerator<T>( getRandom(), items );
        return new GeneratorBuilderDelegate<DiscreteGenerator<T>>( generator );
    }

    /**
     * DiscreteValuedGenerator
     */
    public <T> GeneratorBuilderDelegate<DiscreteValuedGenerator<T>> discreteValuedGenerator(
            Iterable<Pair<Double, Generator<T>>> items )
    {
        DiscreteGenerator<Generator<T>> discreteGenerator = discreteGenerator( items ).build();
        DiscreteValuedGenerator<T> generator = new DiscreteValuedGenerator<T>( discreteGenerator );
        return new GeneratorBuilderDelegate<DiscreteValuedGenerator<T>>( generator );
    }

    /**
     * DiscreteMultiGenerator
     */
    public <T> GeneratorBuilderDelegate<DiscreteMultiGenerator<T>> discreteMultiGenerator(
            Iterable<Pair<Double, T>> items, Integer amountToRetrieve )
    {
        Generator<Integer> amountToRetrieveGenerator = constantGenerator( amountToRetrieve ).build();
        return discreteMultiGenerator( items, amountToRetrieveGenerator );
    }

    public <T> GeneratorBuilderDelegate<DiscreteMultiGenerator<T>> discreteMultiGenerator(
            Iterable<Pair<Double, T>> pairs, Generator<Integer> amountToRetrieveGenerator )
    {
        DiscreteMultiGenerator<T> generator = new DiscreteMultiGenerator<T>( getRandom(), pairs,
                amountToRetrieveGenerator );
        return new GeneratorBuilderDelegate<DiscreteMultiGenerator<T>>( generator );
    }

    /**
     * DiscreteValuedMultiGenerator
     */
    public <K, V> GeneratorBuilderDelegate<DiscreteValuedMultiGenerator<K, V>> discreteValuedMultiGenerator(
            Iterable<Triple<Double, K, Generator<V>>> items, Integer amountToRetrieve )
    {
        Generator<Integer> amountToRetrieveGenerator = constantGenerator( amountToRetrieve ).build();
        return discreteValuedMultiGenerator( items, amountToRetrieveGenerator );
    }

    public <K, V> GeneratorBuilderDelegate<DiscreteValuedMultiGenerator<K, V>> discreteValuedMultiGenerator(
            Iterable<Triple<Double, K, Generator<V>>> items, Generator<Integer> amountToRetrieveGenerator )
    {
        ArrayList<Pair<Double, Pair<K, Generator<V>>>> probabilityItems = new ArrayList<Pair<Double, Pair<K, Generator<V>>>>();
        for ( Triple<Double, K, Generator<V>> item : items )
        {
            double thingProbability = item._1();
            Pair<K, Generator<V>> thingGeneratorPair = Pair.create( item._2(), item._3() );
            probabilityItems.add( Pair.create( thingProbability, thingGeneratorPair ) );
        }

        DiscreteMultiGenerator<Pair<K, Generator<V>>> discreteMultiGenerator = discreteMultiGenerator(
                probabilityItems, amountToRetrieveGenerator ).build();

        DiscreteValuedMultiGenerator<K, V> generator = new DiscreteValuedMultiGenerator<K, V>( getRandom(),
                discreteMultiGenerator );

        return new GeneratorBuilderDelegate<DiscreteValuedMultiGenerator<K, V>>( generator );
    }

    /**
     * RandomByteIteratorGenerator
     */
    public GeneratorBuilderDelegate<RandomByteIteratorGenerator> randomByteIteratorGenerator( Integer length )
    {
        Generator<Integer> lengthGenerator = constantGenerator( length ).build();
        return randomByteIteratorGenerator( lengthGenerator );
    }

    public GeneratorBuilderDelegate<RandomByteIteratorGenerator> randomByteIteratorGenerator(
            Generator<Integer> lengthGenerator )
    {
        RandomByteIteratorGenerator generator = new RandomByteIteratorGenerator( getRandom(), lengthGenerator );
        return new GeneratorBuilderDelegate<RandomByteIteratorGenerator>( generator );
    }

    /**
     * NaiveBoundedRangeNumberGenerator
     */
    public <T extends Number> NumberGeneratorBuilderDelegate<NaiveBoundedRangeNumberGenerator<T>, T> naiveBoundedRangeNumberGenerator(
            MinMaxGeneratorWrapper<T> lowerBoundGenerator, MinMaxGeneratorWrapper<T> upperBoundGenerator,
            Generator<T> unboundedGenerator )
    {
        NaiveBoundedRangeNumberGenerator<T> boundedGenerator = new NaiveBoundedRangeNumberGenerator<T>( getRandom(),
                unboundedGenerator, lowerBoundGenerator, upperBoundGenerator );
        return new NumberGeneratorBuilderDelegate<NaiveBoundedRangeNumberGenerator<T>, T>( boundedGenerator );
    }

    /**
     * UniformNumberGenerator
     */
    public <T extends Number> NumberGeneratorBuilderDelegate<DynamicRangeUniformNumberGenerator<T>, T> uniformNumberGenerator(
            T lowerBound, T upperBound )
    {
        MinMaxGeneratorWrapper<T> lowerBoundGenerator = constantNumberGenerator( lowerBound ).withMinMax( lowerBound,
                lowerBound ).build();
        MinMaxGeneratorWrapper<T> upperBoundGenerator = constantNumberGenerator( upperBound ).withMinMax( upperBound,
                upperBound ).build();
        return dynamicRangeUniformNumberGenerator( lowerBoundGenerator, upperBoundGenerator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<DynamicRangeUniformNumberGenerator<T>, T> dynamicRangeUniformNumberGenerator(
            MinMaxGeneratorWrapper<T> boundingGenerator )
    {
        return dynamicRangeUniformNumberGenerator( boundingGenerator, boundingGenerator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<DynamicRangeUniformNumberGenerator<T>, T> dynamicRangeUniformNumberGenerator(
            MinMaxGeneratorWrapper<T> lowerBoundGenerator, MinMaxGeneratorWrapper<T> upperBoundGenerator )
    {
        DynamicRangeUniformNumberGenerator<T> generator = new DynamicRangeUniformNumberGenerator<T>( getRandom(),
                lowerBoundGenerator, upperBoundGenerator );
        return new NumberGeneratorBuilderDelegate<DynamicRangeUniformNumberGenerator<T>, T>( generator );
    }

    /**
     * ConstantGenerator
     */
    public <T> GeneratorBuilderDelegate<ConstantGenerator<T>> constantGenerator( T constant )
    {
        ConstantGenerator<T> generator = new ConstantGenerator<T>( getRandom(), constant );
        return new GeneratorBuilderDelegate<ConstantGenerator<T>>( generator );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<ConstantGenerator<T>, T> constantNumberGenerator(
            T constant )
    {
        ConstantGenerator<T> generator = new ConstantGenerator<T>( getRandom(), constant );
        return new NumberGeneratorBuilderDelegate<ConstantGenerator<T>, T>( generator );
    }

    /**
     * CounterGenerator
     */
    public <T extends Number> NumberGeneratorBuilderDelegate<CounterGenerator<T>, T> counterGenerator( T start,
            T incrementBy )
    {
        return boundedCounterGenerator( start, incrementBy, null );
    }

    public <T extends Number> NumberGeneratorBuilderDelegate<CounterGenerator<T>, T> boundedCounterGenerator( T start,
            T incrementBy, T max )
    {
        CounterGenerator<T> generator = new CounterGenerator<T>( getRandom(), start, incrementBy, max );
        return new NumberGeneratorBuilderDelegate<CounterGenerator<T>, T>( generator );
    }

    /**
     * ZipfianNumberGenerator
     */
    public <T extends Number> NumberGeneratorBuilderDelegate<YcsbZipfianNumberGenerator<T>, T> zipfianNumberGenerator(
            T lowerBound, T upperBound )
    {
        return zipfianNumberGenerator( lowerBound, upperBound, YcsbZipfianNumberGenerator.ZIPFIAN_CONSTANT );
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

    /**
     * ExponentialNumberGenerator
     */
    // TODO Generic
    public NumberGeneratorBuilderDelegate<BoundedRangeExponentialNumberGenerator, Long> exponentialGenerator(
            double mean )
    {
        return boundedRangeExponentialNumberGenerator( null, null, mean );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<BoundedRangeExponentialNumberGenerator, Long> boundedRangeExponentialNumberGenerator(
            MinMaxGeneratorWrapper<Long> lowerBoundGenerator, MinMaxGeneratorWrapper<Long> upperBoundGenerator,
            double mean )
    {
        BoundedRangeExponentialNumberGenerator generator = new BoundedRangeExponentialNumberGenerator( getRandom(),
                mean, lowerBoundGenerator, upperBoundGenerator );
        return new NumberGeneratorBuilderDelegate<BoundedRangeExponentialNumberGenerator, Long>( generator );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<BoundedRangeExponentialNumberGenerator, Long> exponentialGenerator(
            double percentile, double range )
    {
        return boundedRangeExponentialNumberGenerator( null, null, percentile, range );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<BoundedRangeExponentialNumberGenerator, Long> boundedRangeExponentialNumberGenerator(
            MinMaxGeneratorWrapper<Long> lowerBoundGenerator, MinMaxGeneratorWrapper<Long> upperBoundGenerator,
            double percentile, double range )
    {
        BoundedRangeExponentialNumberGenerator generator = new BoundedRangeExponentialNumberGenerator( getRandom(),
                percentile, range, lowerBoundGenerator, upperBoundGenerator );
        return new NumberGeneratorBuilderDelegate<BoundedRangeExponentialNumberGenerator, Long>( generator );
    }

    /**
     * ScrambledZipfianGenerator
     */
    // TODO Generic
    // Create a zipfian generator for the specified number of items
    public NumberGeneratorBuilderDelegate<YcsbScrambledZipfianGenerator, Long> scrambledZipfianGenerator(
            Long lowerBound, Long upperBound )
    {
        return scrambledZipfianGenerator( lowerBound, upperBound, YcsbZipfianNumberGenerator.ZIPFIAN_CONSTANT );
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
        YcsbZipfianNumberGenerator<Long> zipfianGenerator = null;
        if ( YcsbZipfianNumberGenerator.ZIPFIAN_CONSTANT == zipfianConstant )
        {
            zipfianGenerator = zipfianNumberGenerator( lowerBound, upperBound, zipfianConstant,
                    YcsbScrambledZipfianGenerator.ZETAN ).build();
        }
        else
        {
            // Slower, has to recompute Zetan
            zipfianGenerator = zipfianNumberGenerator( lowerBound, upperBound, zipfianConstant ).build();
        }
        YcsbScrambledZipfianGenerator generator = new YcsbScrambledZipfianGenerator( getRandom(), lowerBound,
                upperBound, zipfianGenerator );
        return new NumberGeneratorBuilderDelegate<YcsbScrambledZipfianGenerator, Long>( generator );
    }

    /**
     * SkewedLatestGenerator
     */
    // TODO Generic
    public NumberGeneratorBuilderDelegate<YcsbSkewedLatestGenerator, Long> skewedLatestGenerator(
            MinMaxGeneratorWrapper<Long> maxGenerator )
    {
        YcsbZipfianNumberGenerator<Long> zipfianGenerator = zipfianNumberGenerator( 0l, maxGenerator.getMax() ).build();
        YcsbSkewedLatestGenerator generator = new YcsbSkewedLatestGenerator( getRandom(), maxGenerator,
                zipfianGenerator );
        return new NumberGeneratorBuilderDelegate<YcsbSkewedLatestGenerator, Long>( generator );
    }

    /**
     * HotspotGenerator
     */
    // TODO Generic
    public NumberGeneratorBuilderDelegate<YcsbDynamicRangeHotspotGenerator, Long> hotspotGenerator( long lowerBound,
            long upperBound, double hotSetFraction, double hotOperationFraction )
    {
        MinMaxGeneratorWrapper<Long> lowerBoundGenerator = constantNumberGenerator( lowerBound ).withMinMax(
                lowerBound, lowerBound ).build();
        MinMaxGeneratorWrapper<Long> upperBoundGenerator = constantNumberGenerator( upperBound ).withMinMax(
                upperBound, upperBound ).build();
        return dynamicRangeHotspotGenerator( lowerBoundGenerator, upperBoundGenerator, hotSetFraction,
                hotOperationFraction );
    }

    // TODO Generic
    public NumberGeneratorBuilderDelegate<YcsbDynamicRangeHotspotGenerator, Long> dynamicRangeHotspotGenerator(
            MinMaxGeneratorWrapper<Long> lowerBoundGenerator, MinMaxGeneratorWrapper<Long> upperBoundGenerator,
            double hotSetFraction, double hotOperationFraction )
    {
        YcsbDynamicRangeHotspotGenerator generator = new YcsbDynamicRangeHotspotGenerator( getRandom(),
                lowerBoundGenerator, upperBoundGenerator, hotSetFraction, hotOperationFraction );
        return new NumberGeneratorBuilderDelegate<YcsbDynamicRangeHotspotGenerator, Long>( generator );
    }
}
