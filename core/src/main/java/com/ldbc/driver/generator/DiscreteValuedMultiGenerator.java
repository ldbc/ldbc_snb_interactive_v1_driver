package com.ldbc.driver.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.util.Pair;

public class DiscreteValuedMultiGenerator<K, V> extends Generator<Map<K, V>>
{
    private final DiscreteMultiGenerator<Pair<K, Generator<V>>> discreteMultiGenerator;

    DiscreteValuedMultiGenerator( RandomDataGenerator random,
            DiscreteMultiGenerator<Pair<K, Generator<V>>> discreteMultiGenerator )
    {
        super( random );
        this.discreteMultiGenerator = discreteMultiGenerator;
    }

    @Override
    protected Map<K, V> doNext() throws GeneratorException
    {
        Map<K, V> keyedValues = new HashMap<K, V>();
        Set<Pair<K, Generator<V>>> keyedValueGenerators = discreteMultiGenerator.next();
        for ( Pair<K, Generator<V>> keyedValueGenerator : keyedValueGenerators )
        {
            keyedValues.put( keyedValueGenerator._1(), keyedValueGenerator._2().next() );
        }
        return keyedValues;
    }

    @Override
    public String toString()
    {
        return "DiscreteValuedMultiGenerator [discreteMultiGenerator=" + discreteMultiGenerator + "]";
    }

}
