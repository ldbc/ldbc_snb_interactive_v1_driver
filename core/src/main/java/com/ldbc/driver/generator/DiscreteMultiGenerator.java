package com.ldbc.driver.generator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.util.Pair;

public class DiscreteMultiGenerator<T> extends Generator<Set<T>>
{
    // generates the number of items to be selected upon next()
    private final Generator<Integer> amountToRetrieveGenerator;
    private final ArrayList<Pair<Double, T>> itemProbabilities;
    private final double probabilitiesSum;

    DiscreteMultiGenerator( RandomDataGenerator random, Iterable<Pair<Double, T>> itemProbabilities,
            Generator<Integer> amountToRetrieveGenerator )
    {
        super( random );
        if ( false == itemProbabilities.iterator().hasNext() )
        {
            throw new GeneratorException( "DiscreteMultiGenerator cannot be empty" );
        }

        this.itemProbabilities = new ArrayList<Pair<Double, T>>();
        double sum = 0;
        for ( Pair<Double, T> item : itemProbabilities )
        {
            this.itemProbabilities.add( item );
            sum += item._1();
        }
        probabilitiesSum = sum;

        this.amountToRetrieveGenerator = amountToRetrieveGenerator;
    }

    @Override
    protected Set<T> doNext() throws GeneratorException
    {
        int amountToRetrieve = amountToRetrieveGenerator.next();

        if ( 0 > amountToRetrieve || amountToRetrieve > itemProbabilities.size() )
        {
            String errMsg = String.format( "amountToRetrieveGenerator returned %s, which exceeds items.size()[%s]",
                    amountToRetrieve, itemProbabilities.size() );
            throw new GeneratorException( errMsg );
        }

        double remainingProbabilitiesSum = probabilitiesSum;
        Set<T> selectedItems = new HashSet<T>();

        for ( int i = 1; i <= amountToRetrieve; i++ )
        {
            Pair<Double, T> selectedItem = nextSelection( selectedItems, remainingProbabilitiesSum );
            selectedItems.add( selectedItem._2() );
            remainingProbabilitiesSum = remainingProbabilitiesSum - selectedItem._1();
        }

        return selectedItems;
    }

    private Pair<Double, T> nextSelection( Set<T> alreadySelectedItems, double remainingProbabilitiesSum )
    {
        double randomValue = getRandom().nextUniform( 0, 1 );

        for ( Pair<Double, T> item : itemProbabilities )
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
