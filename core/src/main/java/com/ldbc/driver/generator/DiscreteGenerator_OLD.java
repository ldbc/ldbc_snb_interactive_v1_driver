package com.ldbc.driver.generator;

import java.util.Vector;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.util.Pair;

// TODO use DiscreteMultiGenerator internally?
public class DiscreteGenerator_OLD<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final Vector<Pair<Double, GENERATE_TYPE>> itemsProbabilities;
    private final double probabilitiesSum;
    private final RandomDataGenerator random;

    DiscreteGenerator_OLD( RandomDataGenerator random, Iterable<Pair<Double, GENERATE_TYPE>> itemsProbabilities )
    {
        if ( false == itemsProbabilities.iterator().hasNext() )
        {
            throw new GeneratorException( "DiscreteGenerator cannot be empty" );
        }
        this.random = random;
        this.itemsProbabilities = new Vector<Pair<Double, GENERATE_TYPE>>();
        double sum = 0;

        for ( Pair<Double, GENERATE_TYPE> item : itemsProbabilities )
        {
            this.itemsProbabilities.add( item );
            sum += item._1();
        }
        probabilitiesSum = sum;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        double randomValue = random.nextUniform( 0, 1 );

        for ( Pair<Double, GENERATE_TYPE> item : itemsProbabilities )
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
