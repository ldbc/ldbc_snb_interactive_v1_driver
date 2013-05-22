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

package com.ldbc.driver.generator.ycsb;

import org.apache.commons.math3.random.RandomDataGenerator;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.util.HashUtils;

/**
 * A generator of a zipfian distribution. It produces a sequence of items, such
 * that some items are more popular than others, according to a zipfian
 * distribution. When you construct an instance of this class, you specify the
 * number of items in the set to draw from, either by specifying an itemcount
 * (so that the sequence is of items from 0 to itemcount-1) or by specifying a
 * min and a max (so that the sequence is of items from min to max inclusive).
 * After you construct the instance, you can change the number of items by
 * calling nextInt(itemcount) or nextLong(itemcount).
 * 
 * Unlike @ZipfianGenerator, this class scatters the "popular" items across the
 * itemspace. Use this, instead of @ZipfianGenerator, if you don't want the head
 * of the distribution (the popular items) clustered together.
 */
// TODO should this delegate or extend ZipfianGenerator?
public class YcsbScrambledZipfianGenerator extends Generator<Long>
{
    public static final double ZETAN = 26.46902820178302;
    public static final long ITEM_COUNT = 10000000000L;

    YcsbZipfianNumberGenerator<Long> zipfianGenerator;
    long min, max, itemCount;

    /**
     * Create a zipfian generator for items between min and max (inclusive) for
     * the specified zipfian constant. If you use a zipfian constant other than
     * 0.99, this will take a long time to complete because we need to recompute
     * zeta.
     * 
     * @param min The smallest integer to generate in the sequence.
     * @param max The largest integer to generate in the sequence.
     * @param zipfianConstant The zipfian constant to use.
     */
    public YcsbScrambledZipfianGenerator( RandomDataGenerator random, long min, long max,
            YcsbZipfianNumberGenerator<Long> zipfianGenerator )
    {
        super( random );
        this.min = min;
        this.max = max;
        this.itemCount = max - min + 1;
        this.zipfianGenerator = zipfianGenerator;
    }

    /**************************************************************************************************/

    /**
     * Return the next long in the sequence.
     */
    protected Long doNext()
    {
        long ret = zipfianGenerator.next();
        ret = min + HashUtils.FNVhash64( ret ) % itemCount;
        return ret;
    }

    // /**
    // * since the values are scrambled (hopefully uniformly), the mean is
    // simply
    // * the middle of the range.
    // */
    // public double mean()
    // {
    // return ( (double) ( ( (long) min ) + (long) max ) ) / 2.0;
    // }
}
