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

import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;

/**
 * Generate a popularity distribution of items, skewed to favor recent items
 * significantly more than older items.
 */
public class SkewedLatestGenerator extends Generator<Long> implements HasMean
{
    CounterGenerator basis;
    ZipfianGenerator zipfian;

    public SkewedLatestGenerator( Random random, CounterGenerator basis ) throws WorkloadException
    {
        super( random );
        this.basis = basis;
        zipfian = new ZipfianGenerator( random, basis.last() );
    }

    /**
     * Generate the next string in the distribution, skewed Zipfian favoring the
     * items most recently returned by the basis generator.
     * 
     * @throws WorkloadException
     */
    @Override
    protected Long doNext() throws WorkloadException
    {
        long max = basis.last();
        // TODO ZipfianGenerator needs parameterized next, e.g.next(max)?
        // return max - _zipfian.next( max );
        return max - zipfian.next();
    }

    // TODO is this a lame halftest?
    // TODO turn into test
    public static void main( String[] args ) throws WorkloadException
    {
        SkewedLatestGenerator generator = new SkewedLatestGenerator( Utils.random(), new CounterGenerator(
                Utils.random(), 1000 ) );
        for ( int i = 0; i < Integer.parseInt( args[0] ); i++ )
        {
            System.out.println( generator.next() );
        }

    }

    public double mean()
    {
        throw new UnsupportedOperationException( "Can't compute mean of non-stationary distribution!" );
    }

}
