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

package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around another DB instance that measures latencies and return codes
 */
public class DBWrapper extends DB
{
    private final DB db;
    private final Measurements measurements;

    private boolean isInitialized;
    private boolean isCleanedUp;

    public DBWrapper( DB db )
    {
        this.db = db;
        this.measurements = Measurements.getMeasurements();
        this.isInitialized = false;
        this.isCleanedUp = false;
    }

    public void setProperties( Map<String, String> properties )
    {
        db.setProperties( properties );
    }

    public Map<String, String> getProperties()
    {
        return db.getProperties();
    }

    /**
     * Called once per DB instance; should be one DB instance per ClientThread
     */
    public void init() throws DBException
    {
        if ( true == isInitialized )
        {
            throw new DBException( "DB may be initialized only once" );
        }
        isInitialized = true;
        db.init();
    }

    /**
     * Called once per DB instance; should be one DB instance per ClientThread
     */
    public void cleanup() throws DBException
    {
        if ( true == isCleanedUp )
        {
            throw new DBException( "DB may be cleaned up only once" );
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
    public int read( String table, String key, Set<String> fields, HashMap<String, ByteIterator> result )
    {
        long startTime = System.nanoTime();
        int resultCode = db.read( table, key, fields, result );
        long endTime = System.nanoTime();
        measurements.measure( "READ", (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( "READ", resultCode );
        return resultCode;
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * 
     * @param table The name of the table
     * @param startKey The record key of the first record to read.
     * @param recordCount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set
     *            field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan( String table, String startKey, int recordCount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result )
    {
        long startTime = System.nanoTime();
        int resultCode = db.scan( table, startKey, recordCount, fields, result );
        long endTime = System.nanoTime();
        measurements.measure( "SCAN", (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( "SCAN", resultCode );
        return resultCode;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     * 
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update( String table, String key, HashMap<String, ByteIterator> values )
    {
        long startTime = System.nanoTime();
        int resultCode = db.update( table, key, values );
        long endTime = System.nanoTime();
        measurements.measure( "UPDATE", (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( "UPDATE", resultCode );
        return resultCode;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     * 
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert( String table, String key, HashMap<String, ByteIterator> values )
    {
        long startTime = System.nanoTime();
        int resultCode = db.insert( table, key, values );
        long endTime = System.nanoTime();
        measurements.measure( "INSERT", (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( "INSERT", resultCode );
        return resultCode;
    }

    /**
     * Delete a record from the database.
     * 
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete( String table, String key )
    {
        long startTime = System.nanoTime();
        int resultCode = db.delete( table, key );
        long endTime = System.nanoTime();
        measurements.measure( "DELETE", (int) ( ( endTime - startTime ) / 1000 ) );
        measurements.reportReturnCode( "DELETE", resultCode );
        return resultCode;
    }

    // TODO add Operation abstraction, generic measure() will be easier then
    // TODO e.g.
    // public int deleteOperationHandler( DeleteOperation operation )
    // {
    // return measureOperation(deleteOperation);
    // }
    // private int measureOperation( BenchmarkOperation operation)
    // {
    // long startTime = System.nanoTime();
    // int resultCode = operation.run();
    // long endTime = System.nanoTime();
    // measurements.measure( "DELETE", (int) ( ( endTime - startTime ) / 1000 )
    // );
    // measurements.reportReturnCode( "DELETE", resultCode );
    // return resultCode;
    // }
}
