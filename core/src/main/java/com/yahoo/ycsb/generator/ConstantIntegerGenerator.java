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

/**
 * A trivial integer generator that always returns the same value.
 * 
 * @author sears
 * 
 */
public class ConstantIntegerGenerator extends Generator<Integer> implements HasMean
{
    private final int constantNumber;

    public ConstantIntegerGenerator( Random random, int constantNumber )
    {
        super( random );
        this.constantNumber = constantNumber;
    }

    @Override
    protected Integer doNext()
    {
        return constantNumber;
    }

    @Override
    public double mean()
    {
        return constantNumber;
    }
}
