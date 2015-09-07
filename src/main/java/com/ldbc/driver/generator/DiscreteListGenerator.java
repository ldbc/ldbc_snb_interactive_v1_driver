package com.ldbc.driver.generator;

import com.ldbc.driver.util.Tuple2;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

public class DiscreteListGenerator<GENERATE_TYPE> extends Generator<List<GENERATE_TYPE>>
{
    // generates the number of items to be selected upon next()
    private final Iterator<Integer> amountToRetrieveGenerator;
    private final ArrayList<Tuple2<Double,GENERATE_TYPE>> itemProbabilities;
    private final double probabilitiesSum;
    private final RandomDataGenerator random;

    DiscreteListGenerator( RandomDataGenerator random, Iterable<Tuple2<Double,GENERATE_TYPE>> itemProbabilities,
            Iterator<Integer> amountToRetrieveGenerator )
    {
        if ( false == itemProbabilities.iterator().hasNext() )
        {
            throw new GeneratorException( "DiscreteMultiGenerator cannot be empty" );
        }

        this.random = random;
        this.itemProbabilities = new ArrayList<Tuple2<Double,GENERATE_TYPE>>();
        double sum = 0;
        for ( Tuple2<Double,GENERATE_TYPE> item : itemProbabilities )
        {
            this.itemProbabilities.add( item );
            sum += item._1();
        }
        probabilitiesSum = sum;

        this.amountToRetrieveGenerator = amountToRetrieveGenerator;
    }

    @Override
    protected List<GENERATE_TYPE> doNext() throws GeneratorException
    {
        int amountToRetrieve = amountToRetrieveGenerator.next();

        if ( 0 > amountToRetrieve || amountToRetrieve > itemProbabilities.size() )
        {
            String errMsg = format( "amountToRetrieveGenerator returned %s, which exceeds number of items [%s]",
                    amountToRetrieve, itemProbabilities.size() );
            throw new GeneratorException( errMsg );
        }

        double remainingProbabilitiesSum = probabilitiesSum;
        List<GENERATE_TYPE> selectedItems = new ArrayList<GENERATE_TYPE>();

        for ( int i = 0; i < amountToRetrieve; i++ )
        {
            Tuple2<Double,GENERATE_TYPE> selectedItem = nextSelection( selectedItems, remainingProbabilitiesSum );
            selectedItems.add( selectedItem._2() );
            remainingProbabilitiesSum = remainingProbabilitiesSum - selectedItem._1();
        }

        return selectedItems;
    }

    private Tuple2<Double,GENERATE_TYPE> nextSelection( List<GENERATE_TYPE> alreadySelectedItems,
            double remainingProbabilitiesSum )
    {
        double randomValue = random.nextUniform( 0, 1 );

        for ( Tuple2<Double,GENERATE_TYPE> item : itemProbabilities )
        {
            if ( true == alreadySelectedItems.contains( item._2() ) )
            {
                continue;
            }
            if ( randomValue < (item._1() / remainingProbabilitiesSum) )
            {
                return item;
            }
            randomValue = randomValue - (item._1() / remainingProbabilitiesSum);
        }

        String errMsg = format( "val=%s\n" + "probabilitiesSum=%s\n" + "remainingProbabilitiesSum=%s\n"
                                + "items=%s\n" + "alreadySelectedItems=%s", randomValue, probabilitiesSum,
                remainingProbabilitiesSum, itemProbabilities.toString(), alreadySelectedItems.toString() );
        throw new GeneratorException( format( "Unexpected Error - should never get to this line\n%s", errMsg ) );
    }

    @Override
    public String toString()
    {
        return "DiscreteListGenerator [items=" + itemProbabilities.toString() + "]";
    }
}
