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

package com.ldbc.generator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.math3.random.RandomDataGenerator;

public abstract class Generator<T> implements Iterator<T>
{
    private T next = null;
    private final RandomDataGenerator random;

    protected Generator( RandomDataGenerator random )
    {
        this.random = random;
    }

    // Return null if nothing more to generate
    protected abstract T doNext() throws GeneratorException;

    // TODO synchronized for now as Generators are shared among threads
    // TODO re-architect framework to not share Generators across threads
    public final synchronized T next()
    {
        next = ( next == null ) ? doNext() : next;
        if ( null == next ) throw new NoSuchElementException( "Generator has nothing more to generate" );
        T tempNext = next;
        next = null;
        return tempNext;
    }

    @Override
    public final boolean hasNext()
    {
        next = ( next == null ) ? doNext() : next;
        return ( next != null );
    }

    @Override
    public final void remove()
    {
        throw new UnsupportedOperationException( "Iterator.remove() not supported by Generator" );
    }

    protected final RandomDataGenerator getRandom()
    {
        return random;
    }

    @Override
    public String toString()
    {
        return "Generator [next=" + next + ", random=" + random + "]";
    }
}
