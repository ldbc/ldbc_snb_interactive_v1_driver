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
import com.yahoo.ycsb.generator.GeneratorException;

/**
 * Generate integers resembling a hotspot distribution where x% of operations
 * access y% of data items. The parameters specify the bounds for the numbers,
 * the percentage of the of the interval which comprises the hot set and the
 * percentage of operations that access the hot set. Numbers of the hot set are
 * always smaller than any number in the cold set. Elements from the hot set and
 * the cold set are chose using a uniform distribution.
 * 
 * @author sudipto
 * 
 */
public class HotspotGenerator extends Generator<Long>
{

    private final long lowerBound;
    private final long upperBound;
    private final long hotInterval;
    private final long coldInterval;
    private final double hotsetFraction;
    private final double hotOpnFraction;

    /**
     * @param lowerBound lower bound of the distribution
     * @param upperBound upper bound of the distribution
     * @param hotsetFraction percentage of data item
     * @param hotOpnFraction percentage of operations accessing the hot set
     */
    public HotspotGenerator( RandomDataGenerator random, long lowerBound, long upperBound, double hotsetFraction,
            double hotOpnFraction )
    {
        super( random );
        if ( hotsetFraction < 0.0 || hotsetFraction > 1.0 )
        {
            throw new GeneratorException( String.format( "Hotset fraction [%s] is out of range[0.0-1.0]",
                    hotsetFraction ) );
        }
        if ( hotOpnFraction < 0.0 || hotOpnFraction > 1.0 )
        {
            throw new GeneratorException( String.format( "Hot operation fraction [%s] is out of range[0.0-1.0]",
                    hotOpnFraction ) );
        }
        if ( lowerBound > upperBound )
        {
            throw new GeneratorException( String.format( "Upper bound[%s] is smaller than lower bound[%s]", upperBound,
                    lowerBound ) );
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.hotsetFraction = hotsetFraction;
        long interval = upperBound - lowerBound + 1;
        this.hotInterval = (int) ( interval * hotsetFraction );
        this.coldInterval = interval - hotInterval;
        this.hotOpnFraction = hotOpnFraction;
    }

    @Override
    protected Long doNext()
    {
        long value = 0;
        if ( getRandom().nextUniform( 0, 1 ) < hotOpnFraction )
        {
            // choose a value from the hot set
            value = lowerBound + getRandom().nextLong( 0, hotInterval );
        }
        else
        {
            // choose a value from the cold set
            value = lowerBound + hotInterval + getRandom().nextLong( 0, coldInterval );
        }
        return value;
    }

    public long getLowerBound()
    {
        return lowerBound;
    }

    public long getUpperBound()
    {
        return upperBound;
    }

    public double getHotsetFraction()
    {
        return hotsetFraction;
    }

    public double getHotOpnFraction()
    {
        return hotOpnFraction;
    }

    public long getColdInterval()
    {
        return coldInterval;
    }

    public long getHotInterval()
    {
        return hotInterval;
    }
}
