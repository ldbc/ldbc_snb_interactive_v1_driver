package com.yahoo.ycsb.generator;

import java.util.Vector;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.util.Pair;

public class DiscreteGenerator<T> extends Generator<T>
{
    private final Vector<Pair<Double, T>> items;
    private final double probabilitiesSum;

    DiscreteGenerator( RandomDataGenerator random, Iterable<Pair<Double, T>> discreteItems )
    {
        super( random );
        if ( false == discreteItems.iterator().hasNext() )
        {
            throw new GeneratorException( "DiscreteGenerator cannot be empty" );
        }
        this.items = new Vector<Pair<Double, T>>();
        double sum = 0;

        for ( Pair<Double, T> item : discreteItems )
        {
            this.items.add( item );
            sum += item._1();
        }
        probabilitiesSum = sum;
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        double val = getRandom().nextUniform( 0, 1 );

        for ( Pair<Double, T> item : items )
        {
            if ( val < item._1() / probabilitiesSum )
            {
                return item._2();
            }
            val -= item._1() / probabilitiesSum;
        }

        throw new GeneratorException( "Unexpected Error - DiscreteGenerator.next() should never get to this line" );
    }

    @Override
    public String toString()
    {
        return "DiscreteGenerator [items=" + items.toString() + "]";
    }
}
