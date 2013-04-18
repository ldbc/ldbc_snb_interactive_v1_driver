/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb.generator;

import java.util.Vector;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.Pair;

/**
 * Chooses from a discrete set of values according to some distribution
 */
public class DiscreteGenerator<T> extends Generator<T>
{
    private final Vector<Pair<Double, T>> items;
    private final double probabilitiesSum;

    DiscreteGenerator( RandomDataGenerator random, Pair<Double, T>... discreteItems )
    {
        super( random );
        if ( 0 == discreteItems.length ) throw new GeneratorException( "DiscreteGenerator cannot be empty" );
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
