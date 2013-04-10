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

public class UniformDoubleGenerator extends Generator<Double>
{
    private final double lowerBound;
    private final double upperBound;

    UniformDoubleGenerator( RandomDataGenerator random, double lowerBound, double upperBound )
    {
        super( random );
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    protected Double doNext()
    {
        return getRandom().nextUniform( lowerBound, upperBound );
    }

    // public double mean()
    // {
    // return ( lowerBound + upperBound ) / 2.0;
    // }
}
