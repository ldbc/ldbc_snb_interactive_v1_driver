package com.ldbc.driver.generator;

import java.util.Vector;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.util.Pair;

// TODO use DiscreteMultiGenerator internally?
public class DiscreteGenerator<T> extends Generator<T>
{
    private final Vector<Pair<Double, T>> itemsProbabilities;
    private final double probabilitiesSum;

    DiscreteGenerator( RandomDataGenerator random, Iterable<Pair<Double, T>> itemsProbabilities )
    {
        super( random );
        if ( false == itemsProbabilities.iterator().hasNext() )
        {
            throw new GeneratorException( "DiscreteGenerator cannot be empty" );
        }
        this.itemsProbabilities = new Vector<Pair<Double, T>>();
        double sum = 0;

        for ( Pair<Double, T> item : itemsProbabilities )
        {
            this.itemsProbabilities.add( item );
            sum += item._1();
        }
        probabilitiesSum = sum;
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        double randomValue = getRandom().nextUniform( 0, 1 );

        for ( Pair<Double, T> item : itemsProbabilities )
        {
            if ( randomValue < item._1() / probabilitiesSum )
            {
                return item._2();
            }
            randomValue = randomValue - ( item._1() / probabilitiesSum );
        }

        throw new GeneratorException( "Unexpected Error - should never get to this line" );
    }

    @Override
    public String toString()
    {
        return "DiscreteGenerator [items=" + itemsProbabilities.toString() + "]";
    }
}
