package com.ldbc.driver.generator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.util.Tuple.Tuple2;

public class DiscreteSetGenerator<GENERATE_TYPE> extends Generator<Set<GENERATE_TYPE>>
{
    // generates the number of items to be selected upon next()
    private final Generator<Integer> amountToRetrieveGenerator;
    private final ArrayList<Tuple2<Double, GENERATE_TYPE>> itemProbabilities;
    private final double probabilitiesSum;
    private final RandomDataGenerator random;

    DiscreteSetGenerator( RandomDataGenerator random, Iterable<Tuple2<Double, GENERATE_TYPE>> itemProbabilities,
            Generator<Integer> amountToRetrieveGenerator )
    {
        if ( false == itemProbabilities.iterator().hasNext() )
        {
            throw new GeneratorException( "DiscreteMultiGenerator cannot be empty" );
        }

        this.random = random;
        this.itemProbabilities = new ArrayList<Tuple2<Double, GENERATE_TYPE>>();
        double sum = 0;
        for ( Tuple2<Double, GENERATE_TYPE> item : itemProbabilities )
        {
            this.itemProbabilities.add( item );
            sum += item._1();
        }
        probabilitiesSum = sum;

        this.amountToRetrieveGenerator = amountToRetrieveGenerator;
    }

    @Override
    protected Set<GENERATE_TYPE> doNext() throws GeneratorException
    {
        int amountToRetrieve = amountToRetrieveGenerator.next();

        if ( 0 > amountToRetrieve || amountToRetrieve > itemProbabilities.size() )
        {
            String errMsg = String.format( "amountToRetrieveGenerator returned %s, which exceeds number of items [%s]",
                    amountToRetrieve, itemProbabilities.size() );
            throw new GeneratorException( errMsg );
        }

        double remainingProbabilitiesSum = probabilitiesSum;
        Set<GENERATE_TYPE> selectedItems = new HashSet<GENERATE_TYPE>();

        for ( int i = 0; i < amountToRetrieve; i++ )
        {
            Tuple2<Double, GENERATE_TYPE> selectedItem = nextSelection( selectedItems, remainingProbabilitiesSum );
            selectedItems.add( selectedItem._2() );
            remainingProbabilitiesSum = remainingProbabilitiesSum - selectedItem._1();
        }

        return selectedItems;
    }

    private Tuple2<Double, GENERATE_TYPE> nextSelection( Set<GENERATE_TYPE> alreadySelectedItems,
            double remainingProbabilitiesSum )
    {
        double randomValue = random.nextUniform( 0, 1 );

        for ( Tuple2<Double, GENERATE_TYPE> item : itemProbabilities )
        {
            if ( true == alreadySelectedItems.contains( item._2() ) )
            {
                continue;
            }
            if ( randomValue < ( item._1() / remainingProbabilitiesSum ) )
            {
                return item;
            }
            randomValue = randomValue - ( item._1() / remainingProbabilitiesSum );
        }

        String errMsg = String.format( "val=%s\n" + "probabilitiesSum=%s\n" + "remainingProbabilitiesSum=%s\n"
                                       + "items=%s\n" + "alreadySelectedItems=%s", randomValue, probabilitiesSum,
                remainingProbabilitiesSum, itemProbabilities.toString(), alreadySelectedItems.toString() );
        throw new GeneratorException( String.format( "Unexpected Error - should never get to this line\n%s", errMsg ) );
    }

    @Override
    public String toString()
    {
        return "DiscreteMultiGenerator [items=" + itemProbabilities.toString() + "]";
    }
}
