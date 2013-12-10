package com.ldbc.driver.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.data.LimitGenerator;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.driver.util.Tuple.Tuple3;

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
     * ---------------------------------------- DECORATORS ------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    /**
     * MinMaxGenerator
     */
    public <T extends Number> MinMaxGenerator<T> minMaxGenerator( Iterator<T> generator, T initialMin, T initialMax )
    {
        return new MinMaxGenerator<T>( generator, initialMin, initialMax );
    }

    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- GENERATORS ------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    /**
     * PrefixGenerator
     */
    public <T extends Number> Iterator<String> prefix( Iterator<T> generator, String prefix )
    {
        return new PrefixGenerator( generator, prefix );
    }

    /**
     * CappedGenerator
     */
    public <T> Iterator<T> limit( Iterator<T> generator, long limit )
    {
        return new LimitGenerator<T>( generator, limit );
    }

    /**
     * DiscreteGenerator
     */
    public <T> Iterator<T> discreteGenerator( Iterable<T> items )
    {
        List<Tuple2<Double, T>> weightedItems = new ArrayList<Tuple2<Double, T>>();
        for ( T item : items )
        {
            weightedItems.add( Tuple.tuple2( 1d, item ) );
        }
        return weightedDiscreteGenerator( weightedItems );
    }

    public <T> Iterator<T> weightedDiscreteGenerator( Iterable<Tuple2<Double, T>> items )
    {
        return new IterableDereferencingGenerator<T>( weightedDiscreteSetGenerator( items, 1 ) );
    }

    /**
     * DiscreteDereferencingGenerator
     */
    public <T> Iterator<T> weightedDiscreteDereferencingGenerator( Iterable<Tuple2<Double, Iterator<T>>> items )
    {
        Iterator<Iterator<T>> discreteGenerator = weightedDiscreteGenerator( items );
        return new IteratorDereferencingGenerator<T>( discreteGenerator );
    }

    /**
     * DiscreteSetGenerator
     */
    public <T> Iterator<Set<T>> weightedDiscreteSetGenerator( Iterable<Tuple2<Double, T>> items,
            Integer amountToRetrieve )
    {
        Iterator<Integer> amountToRetrieveGenerator = constantGenerator( amountToRetrieve );
        return weightedDiscreteSetGenerator( items, amountToRetrieveGenerator );
    }

    public <T> Iterator<Set<T>> weightedDiscreteSetGenerator( Iterable<Tuple2<Double, T>> pairs,
            Iterator<Integer> amountToRetrieveGenerator )
    {
        return new DiscreteSetGenerator<T>( getRandom(), pairs, amountToRetrieveGenerator );
    }

    /**
     * DiscreteMapGenerator
     */
    public <K, V> Iterator<Map<K, V>> weightedDiscreteMapGenerator( Iterable<Tuple3<Double, K, Iterator<V>>> items,
            Integer amountToRetrieve )
    {
        Iterator<Integer> amountToRetrieveGenerator = constantGenerator( amountToRetrieve );
        return weightedDiscreteMapGenerator( items, amountToRetrieveGenerator );
    }

    public <K, V> Iterator<Map<K, V>> weightedDiscreteMapGenerator( Iterable<Tuple3<Double, K, Iterator<V>>> items,
            Iterator<Integer> amountToRetrieveGenerator )
    {
        List<Tuple2<Double, Tuple2<K, Iterator<V>>>> probabilityItems = new ArrayList<Tuple2<Double, Tuple2<K, Iterator<V>>>>();
        for ( Tuple3<Double, K, Iterator<V>> item : items )
        {
            double thingProbability = item._1();
            Tuple2<K, Iterator<V>> thingGeneratorPair = Tuple.tuple2( item._2(), item._3() );
            probabilityItems.add( Tuple.tuple2( thingProbability, thingGeneratorPair ) );
        }

        Iterator<Set<Tuple2<K, Iterator<V>>>> discreteSetGenerator = weightedDiscreteSetGenerator( probabilityItems,
                amountToRetrieveGenerator );

        Function1<Set<Tuple2<K, Iterator<V>>>, Map<K, V>> pairsToMap = new Function1<Set<Tuple2<K, Iterator<V>>>, Map<K, V>>()
        {
            @Override
            public Map<K, V> apply( Set<Tuple2<K, Iterator<V>>> pairs )
            {
                Map<K, V> keyedValues = new HashMap<K, V>();
                for ( Tuple2<K, Iterator<V>> pair : pairs )
                {
                    keyedValues.put( pair._1(), pair._2().next() );
                }
                return keyedValues;
            }
        };
        return new MappingGenerator<Set<Tuple2<K, Iterator<V>>>, Map<K, V>>( discreteSetGenerator, pairsToMap );
    }

    /**
     * RandomByteIteratorGenerator
     */
    public Iterator<ByteIterator> randomByteIteratorGenerator( Integer length )
    {
        Iterator<Integer> lengthGenerator = constantGenerator( length );
        return randomByteIteratorGenerator( lengthGenerator );
    }

    public Iterator<ByteIterator> randomByteIteratorGenerator( Iterator<Integer> lengthGenerator )
    {
        return new RandomByteIteratorGenerator( getRandom(), lengthGenerator );
    }

    /**
     * NaiveBoundedRangeNumberGenerator
     */
    public <T extends Number> Iterator<T> naiveBoundedRangeNumberGenerator( MinMaxGenerator<T> lowerBoundGenerator,
            MinMaxGenerator<T> upperBoundGenerator, Iterator<T> unboundedGenerator )
    {
        return new NaiveBoundedRangeNumberGenerator<T>( unboundedGenerator, lowerBoundGenerator, upperBoundGenerator );
    }

    /**
     * UniformNumberGenerator
     */
    public <T extends Number> Iterator<T> uniformNumberGenerator( T lowerBound, T upperBound )
    {
        MinMaxGenerator<T> lowerBoundGenerator = minMaxGenerator( constantNumberGenerator( lowerBound ), lowerBound,
                lowerBound );
        MinMaxGenerator<T> upperBoundGenerator = minMaxGenerator( constantNumberGenerator( upperBound ), upperBound,
                upperBound );
        return dynamicRangeUniformNumberGenerator( lowerBoundGenerator, upperBoundGenerator );
    }

    public <T extends Number> Iterator<T> dynamicRangeUniformNumberGenerator( MinMaxGenerator<T> boundingGenerator )
    {
        return dynamicRangeUniformNumberGenerator( boundingGenerator, boundingGenerator );
    }

    public <T extends Number> Iterator<T> dynamicRangeUniformNumberGenerator( MinMaxGenerator<T> lowerBoundGenerator,
            MinMaxGenerator<T> upperBoundGenerator )
    {
        return new DynamicRangeUniformNumberGenerator<T>( getRandom(), lowerBoundGenerator, upperBoundGenerator );
    }

    /**
     * ConstantGenerator
     */
    public <T> Iterator<T> constantGenerator( T constant )
    {
        return new ConstantGenerator<T>( constant );
    }

    public <T extends Number> Iterator<T> constantNumberGenerator( T constant )
    {
        return new ConstantGenerator<T>( constant );
    }

    /**
     * IncrementingGenerator
     */

    public <T extends Number> Iterator<T> incrementingGenerator( T start, T incrementBy )
    {
        return boundedIncrementingGenerator( start, new ConstantGenerator<T>( incrementBy ), null );
    }

    public <T extends Number> Iterator<T> incrementingGenerator( T start, Iterator<T> incrementByGenerator )
    {
        return boundedIncrementingGenerator( start, incrementByGenerator, null );
    }

    public <T extends Number> Iterator<T> boundedIncrementingGenerator( T start, T incrementBy, T max )
    {
        return boundedIncrementingGenerator( start, new ConstantGenerator<T>( incrementBy ), max );
    }

    public <T extends Number> Iterator<T> boundedIncrementingGenerator( T start, Iterator<T> incrementByGenerator, T max )
    {
        return new IncrementingGenerator<T>( start, incrementByGenerator, max );
    }

    /**
     * ExponentialNumberGenerator
     */
    public <T extends Number> Iterator<T> exponentialGenerator( T mean )
    {
        return new ExponentialNumberGenerator<T>( getRandom(), mean );
    }

    public <T extends Number> Iterator<T> boundedRangeExponentialNumberGenerator(
            MinMaxGenerator<T> lowerBoundGenerator, MinMaxGenerator<T> upperBoundGenerator, T mean )
    {
        Iterator<T> generator = new ExponentialNumberGenerator<T>( getRandom(), mean );
        return naiveBoundedRangeNumberGenerator( lowerBoundGenerator, upperBoundGenerator, generator );
    }
}
