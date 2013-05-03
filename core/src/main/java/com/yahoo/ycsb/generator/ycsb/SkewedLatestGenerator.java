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

package com.yahoo.ycsb.generator.ycsb;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.MinMaxGeneratorWrapper;

/**
 * Skewed distribution to favor recent items significantly more than older items
 */
public class SkewedLatestGenerator extends Generator<Long>
{
    private final MinMaxGeneratorWrapper<Long> maxGenerator;
    private final ZipfianGenerator zipfianGenerator;

    public SkewedLatestGenerator( RandomDataGenerator random, MinMaxGeneratorWrapper<Long> maxGenerator,
            ZipfianGenerator zipfianGenerator )
    {
        super( random );
        this.maxGenerator = maxGenerator;
        this.zipfianGenerator = zipfianGenerator;
    }

    @Override
    protected Long doNext()
    {
        long max = maxGenerator.getMax();
        // TODO ZipfianGenerator needs parameterized next, e.g.next(max)?
        // return max - _zipfian.next( max );
        return max - zipfianGenerator.next();
    }
}
