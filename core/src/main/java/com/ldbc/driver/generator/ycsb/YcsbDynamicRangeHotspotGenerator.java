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
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.wrapper.MinMaxGeneratorWrapper;

/**
 * Generate integrals resembling hot-spot distribution where x% of operations
 * access y% of data items. The parameters specify the bounds for the numbers,
 * the percentage of the of the interval which comprises the hot set and the
 * percentage of operations that access the hot set. Numbers of the hot set are
 * always smaller than any number in the cold set. Elements from the hot set and
 * the cold set are chose using a uniform distribution.
 * 
 * <---------[hlb--------------hub][clb-----------cub]--------------->
 * <---------HHHHHHHHHHHHHHHHHHHHHHCCCCCCCCCCCCCCCCCCC--------------->
 * 
 * @author sudipto
 * @author Alex Averbuch (alex.averbuch@gmail.com)
 * 
 */
// TODO Generalize class to accept multiple ranges, with probabilities for each
// TODO HotSpotsGenerator
public class YcsbDynamicRangeHotspotGenerator extends Generator<Long>
{
    private final double hotOperationFraction; // (0.0,1.0)
    private final double hotSetFraction; // (0.0,1.0)

    private final MinMaxGeneratorWrapper<Long> lowerBoundGenerator;
    private final MinMaxGeneratorWrapper<Long> upperBoundGenerator;

    /**
     * @param lowerBound distribution lower bound
     * @param upperBound distribution upper bound
     * @param hotSetFraction percentage of data items
     * @param hotOperationFraction percentage of operations accessing hot set
     */
    public YcsbDynamicRangeHotspotGenerator( RandomDataGenerator random,
            MinMaxGeneratorWrapper<Long> lowerBoundGenerator, MinMaxGeneratorWrapper<Long> upperBoundGenerator,
            double hotSetFraction, double hotOperationFraction )
    {
        super( random );
        if ( 0.0 > hotSetFraction || hotSetFraction > 1.0 )
        {
            throw new GeneratorException( String.format( "Hotset fraction [%s] is out of range(0.0,1.0)",
                    hotSetFraction ) );
        }
        if ( 0.0 > hotOperationFraction || hotOperationFraction > 1.0 )
        {
            throw new GeneratorException( String.format( "Hot operation fraction [%s] is out of range(0.0,1.0)",
                    hotOperationFraction ) );
        }
        if ( lowerBoundGenerator.getMax() > upperBoundGenerator.getMax() )
        {
            throw new GeneratorException( String.format( "Upper bound[%s] is smaller than lower bound[%s]",
                    upperBoundGenerator.getMax(), lowerBoundGenerator.getMax() ) );
        }

        this.lowerBoundGenerator = lowerBoundGenerator;
        this.upperBoundGenerator = upperBoundGenerator;

        this.hotSetFraction = hotSetFraction;
        this.hotOperationFraction = hotOperationFraction;
    }

    @Override
    protected Long doNext()
    {
        if ( getRandom().nextUniform( 0, 1 ) < hotOperationFraction )
        {
            return getFromHotSet();
        }
        else
        {
            return getFromColdSet();
        }
    }

    private Long getFromHotSet()
    {
        return getRandom().nextLong( getHotSetLowerBound(), getHotSetUpperBound() );
    }

    private Long getFromColdSet()
    {
        return getRandom().nextLong( getColdSetLowerBound(), getColdSetUpperBound() );
    }

    private Long getHotInterval()
    {
        long interval = getUpperBound() - getLowerBound() + 1;
        return Math.round( interval * hotSetFraction );
    }

    private Long getHotSetLowerBound()
    {
        return getLowerBound();
    }

    private Long getHotSetUpperBound()
    {
        return getHotSetLowerBound() + getHotInterval() - 1;
    }

    private Long getColdSetLowerBound()
    {
        return getHotSetUpperBound() + 1;
    }

    private Long getColdSetUpperBound()
    {
        return getUpperBound();
    }

    private Long getLowerBound()
    {
        return lowerBoundGenerator.getMax();
    }

    private Long getUpperBound()
    {
        return upperBoundGenerator.getMax();
    }
}
