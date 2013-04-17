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

public class UniformNumberGenerator<T extends Number> extends Generator<T>
{
    private final T lowerBound;
    private final T upperBound;
    private final NextDelegate<T> nextDelegate;

    UniformNumberGenerator( RandomDataGenerator random, T lowerBound, T upperBound )
    {
        super( random );
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.nextDelegate = createNextDelegate( lowerBound.getClass() );
    }

    @Override
    protected T doNext()
    {
        return nextDelegate.next( lowerBound, upperBound, getRandom() );
    }

    // public double mean()
    // {
    // return ( lowerBound + upperBound ) / 2.0;
    // }

    private NextDelegate<T> createNextDelegate( Class<? extends Number> type )
    {
        /* 
         * Supported: Double, Integer, Long
         */
        if ( type.isAssignableFrom( Integer.class ) ) return (NextDelegate<T>) new IntegerDelegate();
        if ( type.isAssignableFrom( Long.class ) ) return (NextDelegate<T>) new LongDelegate();
        if ( type.isAssignableFrom( Double.class ) ) return (NextDelegate<T>) new DoubleDelegate();
        /*  
         * Not supported: Byte, Float, Short, AtomicInteger, AtomicLong, BigDecimal, BigInteger
         */
        throw new GeneratorException( String.format( "%s not supported. Only supports: Double, Integer, Long",
                type.getName() ) );
    }

    private abstract class NextDelegate<T1 extends Number>
    {
        public abstract T1 next( T1 lb, T1 ub, RandomDataGenerator random );
    }

    private class IntegerDelegate extends NextDelegate<Integer>
    {
        @Override
        public Integer next( Integer lb, Integer ub, RandomDataGenerator random )
        {
            return (int) Math.round( random.nextUniform( lb, ub ) );
        }
    }

    private class LongDelegate extends NextDelegate<Long>
    {
        @Override
        public Long next( Long lb, Long ub, RandomDataGenerator random )
        {
            return Math.round( random.nextUniform( lb, ub ) );
        }
    }

    private class DoubleDelegate extends NextDelegate<Double>
    {
        @Override
        public Double next( Double lb, Double ub, RandomDataGenerator random )
        {
            return random.nextUniform( lb, ub );
        }
    }

}
