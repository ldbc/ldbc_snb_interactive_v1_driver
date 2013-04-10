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

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.WorkloadException;

/**
 * Skewed distribution to favor recent items significantly more than older items
 */
public class SkewedLatestGenerator extends Generator<Long>
{
    private CounterGenerator basis;
    private ZipfianGenerator zipfian;

    SkewedLatestGenerator( RandomDataGenerator random, CounterGenerator basis, ZipfianGenerator zipfianGenerator )
                                                                                                                  throws WorkloadException
    {
        super( random );
        this.basis = basis;
        this.zipfian = zipfianGenerator;
    }

    @Override
    protected Long doNext() throws WorkloadException
    {
        long max = basis.last();
        // TODO ZipfianGenerator needs parameterized next, e.g.next(max)?
        // return max - _zipfian.next( max );
        return max - zipfian.next();
    }
}
