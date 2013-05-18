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

package com.ldbc.db2;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ldbc.data.ByteIterator;
import com.ldbc.measurements.Measurements;

/**
 * Wrapper around another DB instance that measures latencies and return codes
 */
public class DbWrapper2 extends Db2
{
    private final Db2 db;
    private final Measurements measurements;

    private boolean isInitialized;
    private boolean isCleanedUp;

    public DbWrapper2( Db2 db )
    {
        this.db = db;
        this.measurements = Measurements.getMeasurements();
        this.isInitialized = false;
        this.isCleanedUp = false;
    }

    @Override
    public void init( Map<String, String> properties ) throws DbException2
    {
        if ( true == isInitialized )
        {
            throw new DbException2( "DB may be initialized only once" );
        }
        isInitialized = true;
        db.init( properties );
    }

    @Override
    public void cleanup() throws DbException2
    {
        if ( true == isCleanedUp )
        {
            throw new DbException2( "DB may be cleaned up only once" );
        }
        isCleanedUp = true;
        long startTime = System.nanoTime();
        db.cleanup();
        long endTime = System.nanoTime();
        measurements.measure( "CLEANUP", (int) ( ( endTime - startTime ) / 1000 ) );
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     * 
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read( String table, String key, Set<String> fields, Map<String, ByteIterator> result )
    {
        long startTime = System.nanoTime();
        int resultCode = db.read( table, key, fields, result );
        long endTime = System.nanoTime();
        measurements.measure( "READ", (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( "READ", resultCode );
        return resultCode;
    }
}
