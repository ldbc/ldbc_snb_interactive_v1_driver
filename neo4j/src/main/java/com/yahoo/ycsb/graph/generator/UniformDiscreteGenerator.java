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

package com.yahoo.ycsb.graph.generator;

import java.util.Vector;

import com.yahoo.ycsb.WorkloadException;

/**
 * An expression that generates a random integer in the specified range
 */
// TODO change to DiscreteGenerator
// do this by converting "values" into Pair items, all with equal value
// TODO subclass UniformDiscreteGenerator
// TODO subclass "Custom"DiscreteGenerator
public class UniformDiscreteGenerator<T> extends Generator<T>
{
    Vector<T> items;
    UniformIntegerGenerator generator;

    /**
     * Generator will return items from the specified set uniformly randomly
     */
    public UniformDiscreteGenerator( Vector<T> values )
    {
        items = (Vector<T>) values.clone();
        // TODO do another way, don't like using subclass in baseclass!
        generator = new UniformIntegerGenerator( 0, values.size() - 1 );
    }

    @Override
    protected T doNext() throws WorkloadException
    {
        return items.elementAt( generator.next() );
    }
}
