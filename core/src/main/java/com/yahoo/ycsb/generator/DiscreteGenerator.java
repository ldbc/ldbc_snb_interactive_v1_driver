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

import java.util.Random;
import java.util.Vector;

import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;

/**
 * Generates a distribution by choosing from a discrete set of values.
 */
// TODO make it work better with UniformDiscreteGenerator
public class DiscreteGenerator extends Generator<Pair<Double, Object>>
{
    private Vector<Pair<Double, Object>> items;

    public DiscreteGenerator( Random random, Pair<Double, Object>... newItems )
    {
        super( random );
        items = new Vector<Pair<Double, Object>>();
        for ( Pair<Double, Object> item : newItems )
        {
            items.add( item );
        }
    }

    @Override
    protected Pair<Double, Object> doNext() throws WorkloadException
    {
        if ( 0 == items.size() ) throw new WorkloadException( "DiscreteGenerator cannot be empty" );

        double sum = 0;

        for ( Pair<Double, Object> item : items )
        {
            sum += item._1();
        }

        double val = Utils.random().nextDouble();

        for ( Pair<Double, Object> item : items )
        {
            if ( val < item._1() / sum )
            {
                return item;
            }

            val -= item._1() / sum;
        }

        throw new WorkloadException( "Unexpected Error - DiscreteGenerator.next() should never get to this line" );
    }

    @Override
    public String toString()
    {
        return "DiscreteGenerator [items=" + items.toString() + "]";
    }
}
