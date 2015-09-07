package com.ldbc.driver.generator;

import com.google.common.collect.Lists;
import com.ldbc.driver.util.Tuple2;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.List;

import static java.lang.String.format;

public class DiscreteGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private final Tuple2<Double,GENERATE_TYPE>[] itemProbabilities;
    private final double probabilitiesSum;
    private final RandomDataGenerator random;

    DiscreteGenerator( RandomDataGenerator random, Iterable<Tuple2<Double,GENERATE_TYPE>> itemProbabilities )
    {
        if ( false == itemProbabilities.iterator().hasNext() )
        {
            throw new GeneratorException( "DiscreteMultiGenerator cannot be empty" );
        }

        this.random = random;
        List<Tuple2<Double,GENERATE_TYPE>> itemProbabilitiesList = Lists.newArrayList( itemProbabilities );
        this.itemProbabilities = itemProbabilitiesList.toArray( new Tuple2[itemProbabilitiesList.size()] );
        double sum = 0;
        for ( int i = 0; i < this.itemProbabilities.length; i++ )
        {
            sum += this.itemProbabilities[i]._1();
        }
        probabilitiesSum = sum;
    }

    @Override
    protected GENERATE_TYPE doNext()
    {
        double randomValue = random.nextUniform( 0, 1 );

        for ( int i = 0; i < itemProbabilities.length; i++ )
        {
            Tuple2<Double,GENERATE_TYPE> item = itemProbabilities[i];
            if ( randomValue < (item._1() / probabilitiesSum) )
            {
                return item._2();
            }
            randomValue = randomValue - (item._1() / probabilitiesSum);
        }

        String errMsg = format( "randomValue=%s\nprobabilitiesSum=%s\nitems=%s\n", randomValue, probabilitiesSum,
                itemProbabilities.toString() );
        throw new GeneratorException( format(
                "Unexpected Error - failed to select next discrete element - should never get to this line\n%s",
                errMsg ) );
    }

    @Override
    public String toString()
    {
        return "DiscreteGenerator [items=" + itemProbabilities.toString() + "]";
    }
}
