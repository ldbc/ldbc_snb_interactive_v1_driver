package com.ldbc.driver.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.generator.wrapper.MinMaxGeneratorWrapper;
import com.ldbc.driver.generator.wrapper.PrefixGeneratorWrapper;
import com.ldbc.driver.util.Function;
import com.ldbc.driver.util.Pair;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.Triple;

public class GeneratorFactory
{

    private final RandomDataGeneratorFactory randomDataGeneratorFactory;

    public GeneratorFactory( RandomDataGeneratorFactory randomDataGeneratorFactory )
    {
        this.randomDataGeneratorFactory = randomDataGeneratorFactory;
    }

    // Every returned Generator will use a different RandomDataGenerator
    private RandomDataGenerator getRandom()
    {
        return randomDataGeneratorFactory.newRandom();
    }

    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- WRAPPERS --------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    /**
     * PrefixGeneratorWrapper
     */
    public <T extends Number> PrefixGeneratorWrapper prefixGeneratorWrapper( Generator<T> generator, String prefix )
    {
        return new PrefixGeneratorWrapper( generator, prefix );
    }

    /**
     * MinMaxGeneratorWrapper
     */
    public <T extends Number> MinMaxGeneratorWrapper<T> minMaxGeneratorWrapper( Generator<T> generator, T initialMin,
            T initialMax )
    {
        return new MinMaxGeneratorWrapper<T>( generator, initialMin, initialMax );
    }

    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- GENERATORS ------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    /**
     * DiscreteGenerator
     */
    public <T> Generator<T> discreteGenerator( Iterable<T> items )
    {
        List<Pair<Double, T>> weightedItems = new ArrayList<Pair<Double, T>>();
        for ( T item : items )
        {
            weightedItems.add( Pair.create( 1d, item ) );
        }
        return weightedDiscreteGenerator( weightedItems );
    }

    public <T> Generator<T> weightedDiscreteGenerator( Iterable<Pair<Double, T>> items )
    {
        return new PoppingGenerator<T>( weightedDiscreteSetGenerator( items, 1 ) );
    }

    /**
     * DiscreteDereferencingGenerator
     */
    public <T> Generator<T> weightedDiscreteDereferencingGenerator( Iterable<Pair<Double, Generator<T>>> items )
    {
        Generator<Generator<T>> discreteGenerator = weightedDiscreteGenerator( items );
        return new DereferencingGenerator<T>( discreteGenerator );
    }

    /**
     * DiscreteSetGenerator
     */
    public <T> Generator<Set<T>> weightedDiscreteSetGenerator( Iterable<Pair<Double, T>> items, Integer amountToRetrieve )
    {
        Generator<Integer> amountToRetrieveGenerator = constantGenerator( amountToRetrieve );
        return weightedDiscreteSetGenerator( items, amountToRetrieveGenerator );
    }

    public <T> Generator<Set<T>> weightedDiscreteSetGenerator( Iterable<Pair<Double, T>> pairs,
            Generator<Integer> amountToRetrieveGenerator )
    {
        return new DiscreteSetGenerator<T>( getRandom(), pairs, amountToRetrieveGenerator );
    }

    /**
     * DiscreteMapGenerator
     */
    public <K, V> Generator<Map<K, V>> weightedDiscreteMapGenerator( Iterable<Triple<Double, K, Generator<V>>> items,
            Integer amountToRetrieve )
    {
        Generator<Integer> amountToRetrieveGenerator = constantGenerator( amountToRetrieve );
        return weightedDiscreteMapGenerator( items, amountToRetrieveGenerator );
    }

    public <K, V> Generator<Map<K, V>> weightedDiscreteMapGenerator( Iterable<Triple<Double, K, Generator<V>>> items,
            Generator<Integer> amountToRetrieveGenerator )
    {
        List<Pair<Double, Pair<K, Generator<V>>>> probabilityItems = new ArrayList<Pair<Double, Pair<K, Generator<V>>>>();
        for ( Triple<Double, K, Generator<V>> item : items )
        {
            double thingProbability = item._1();
            Pair<K, Generator<V>> thingGeneratorPair = Pair.create( item._2(), item._3() );
            probabilityItems.add( Pair.create( thingProbability, thingGeneratorPair ) );
        }

        Generator<Set<Pair<K, Generator<V>>>> discreteSetGenerator = weightedDiscreteSetGenerator( probabilityItems,
                amountToRetrieveGenerator );

        Function<Set<Pair<K, Generator<V>>>, Map<K, V>> pairsToMap = new Function<Set<Pair<K, Generator<V>>>, Map<K, V>>()
        {
            @Override
            public Map<K, V> apply( Set<Pair<K, Generator<V>>> pairs )
            {
                Map<K, V> keyedValues = new HashMap<K, V>();
                for ( Pair<K, Generator<V>> pair : pairs )
                {
                    keyedValues.put( pair._1(), pair._2().next() );
                }
                return keyedValues;
            }
        };
        return new MappingGenerator<Set<Pair<K, Generator<V>>>, Map<K, V>>( discreteSetGenerator, pairsToMap );
    }

    /**
     * RandomByteIteratorGenerator
     */
    public RandomByteIteratorGenerator randomByteIteratorGenerator( Integer length )
    {
        Generator<Integer> lengthGenerator = constantGenerator( length );
        return randomByteIteratorGenerator( lengthGenerator );
    }

    public RandomByteIteratorGenerator randomByteIteratorGenerator( Generator<Integer> lengthGenerator )
    {
        return new RandomByteIteratorGenerator( getRandom(), lengthGenerator );
    }

    /**
     * NaiveBoundedRangeNumberGenerator
     */
    public <T extends Number> NaiveBoundedRangeNumberGenerator<T> naiveBoundedRangeNumberGenerator(
            MinMaxGeneratorWrapper<T> lowerBoundGenerator, MinMaxGeneratorWrapper<T> upperBoundGenerator,
            Generator<T> unboundedGenerator )
    {
        return new NaiveBoundedRangeNumberGenerator<T>( unboundedGenerator, lowerBoundGenerator, upperBoundGenerator );
    }

    /**
     * UniformNumberGenerator
     */
    public <T extends Number> DynamicRangeUniformNumberGenerator<T> uniformNumberGenerator( T lowerBound, T upperBound )
    {
        MinMaxGeneratorWrapper<T> lowerBoundGenerator = minMaxGeneratorWrapper( constantNumberGenerator( lowerBound ),
                lowerBound, lowerBound );
        MinMaxGeneratorWrapper<T> upperBoundGenerator = minMaxGeneratorWrapper( constantNumberGenerator( upperBound ),
                upperBound, upperBound );
        return dynamicRangeUniformNumberGenerator( lowerBoundGenerator, upperBoundGenerator );
    }

    public <T extends Number> DynamicRangeUniformNumberGenerator<T> dynamicRangeUniformNumberGenerator(
            MinMaxGeneratorWrapper<T> boundingGenerator )
    {
        return dynamicRangeUniformNumberGenerator( boundingGenerator, boundingGenerator );
    }

    public <T extends Number> DynamicRangeUniformNumberGenerator<T> dynamicRangeUniformNumberGenerator(
            MinMaxGeneratorWrapper<T> lowerBoundGenerator, MinMaxGeneratorWrapper<T> upperBoundGenerator )
    {
        return new DynamicRangeUniformNumberGenerator<T>( getRandom(), lowerBoundGenerator, upperBoundGenerator );
    }

    /**
     * ConstantGenerator
     */
    public <T> ConstantGenerator<T> constantGenerator( T constant )
    {
        return new ConstantGenerator<T>( constant );
    }

    public <T extends Number> ConstantGenerator<T> constantNumberGenerator( T constant )
    {
        return new ConstantGenerator<T>( constant );
    }

    /**
     * IncrementingGenerator
     */

    public <T extends Number> IncrementingGenerator<T> incrementingGenerator( T start, T incrementBy )
    {
        return boundedIncrementingGenerator( start, new ConstantGenerator<T>( incrementBy ), null );
    }

    public <T extends Number> IncrementingGenerator<T> incrementingGenerator( T start, Generator<T> incrementByGenerator )
    {
        return boundedIncrementingGenerator( start, incrementByGenerator, null );
    }

    public <T extends Number> IncrementingGenerator<T> boundedIncrementingGenerator( T start, T incrementBy, T max )
    {
        return boundedIncrementingGenerator( start, new ConstantGenerator<T>( incrementBy ), max );
    }

    public <T extends Number> IncrementingGenerator<T> boundedIncrementingGenerator( T start,
            Generator<T> incrementByGenerator, T max )
    {
        return new IncrementingGenerator<T>( start, incrementByGenerator, max );
    }

    /**
     * ExponentialNumberGenerator
     */
    public <T extends Number> ExponentialNumberGenerator<T> exponentialGenerator( T mean )
    {
        return new ExponentialNumberGenerator<T>( getRandom(), mean );
    }

    public <T extends Number> NaiveBoundedRangeNumberGenerator<T> boundedRangeExponentialNumberGenerator(
            MinMaxGeneratorWrapper<T> lowerBoundGenerator, MinMaxGeneratorWrapper<T> upperBoundGenerator, T mean )
    {
        Generator<T> generator = new ExponentialNumberGenerator<T>( getRandom(), mean );
        return naiveBoundedRangeNumberGenerator( lowerBoundGenerator, upperBoundGenerator, generator );
    }
}
