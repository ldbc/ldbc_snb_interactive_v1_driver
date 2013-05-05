/**                                                                                                                                                                                
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.                                                                                                                             
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

package com.ldbc.generator.ycsb;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.generator.Generator;

/**
 * Produces a sequence of longs according to an exponential distribution.
 * Smaller intervals are more frequent than larger ones, and there is no bound
 * on the length of an interval.
 * 
 * gamma: mean rate events occur. 1/gamma: half life - average interval length
 */
public class YcsbExponentialGenerator extends Generator<Long>
{
    // % of readings within most recent exponential.frac portion of dataset
    public static final String EXPONENTIAL_PERCENTILE = "exponential.percentile";
    public static final String EXPONENTIAL_PERCENTILE_DEFAULT = "95";

    // Fraction of the dataset accessed exponential.percentile of the time
    public static final String EXPONENTIAL_FRAC = "exponential.frac";
    public static final String EXPONENTIAL_FRAC_DEFAULT = "0.8571428571"; // 1/7

    // Exponential constant
    private double gamma;

    public YcsbExponentialGenerator( RandomDataGenerator random, double mean )
    {
        super( random );
        gamma = 1.0 / mean;
    }

    public YcsbExponentialGenerator( RandomDataGenerator random, double percentile, double range )
    {
        super( random );
        gamma = -Math.log( 1.0 - percentile / 100.0 ) / range;
    }

    @Override
    protected Long doNext()
    {
        return (long) ( -Math.log( getRandom().nextUniform( 0, 1 ) ) / gamma );
    }
}
