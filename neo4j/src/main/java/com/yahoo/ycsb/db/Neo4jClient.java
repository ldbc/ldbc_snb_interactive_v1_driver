/**
 * Neo4j client binding for YCSB.
 *
 * Submitted by Alex Averbuch on 25/02/2013.
 *
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.sun.jersey.api.client.ClientHandlerException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * Neo4j client for YCSB framework.
 * 
 * Properties to set:
 * 
 * neo4j.url=http://localhost:7474/db/data <br>
 * neo4j.primarykey=primarykey <br>
 * neo4j.table=usertable <br>
 * neo4j.clear=false <br>
 * 
 * @author Alex Averbuch
 */
public class Neo4jClient extends DB
{
    private Neo4jClientCommands commands;

    private String url;
    private String primaryKeyProperty;
    private boolean clear;
    // TODO use "table" when 2.0 is released
    private String table;

    // TODO Remove if/when YCSB provides a real logger
    final private Neo4jClientLogger log = new Neo4jClientLogger();

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     * 
     * @throws DBException
     */
    public void init() throws DBException
    {
        try
        {
            // Initialize Neo4j driver
            Properties props = getProperties();
            this.url = props.getProperty( "neo4j.url", "http://localhost:7474/db/data" );
            this.primaryKeyProperty = props.getProperty( "neo4j.primarykey", "primarykey" );
            // TODO use "table" when 2.0 is released
            this.table = props.getProperty( "neo4j.table", "usertable" );
            this.clear = Boolean.parseBoolean( props.getProperty( "neo4j.clear", "false" ) );

            log.info( "*** Neo4j Properties ***" );
            log.info( "table = " + this.table );
            log.info( "primary key = " + this.primaryKeyProperty );
            log.info( "url = " + this.url );
            log.info( "clear database = " + this.clear );
            log.info( "************************" );

            log.info( "Connecting to database: " + this.url );
            this.commands = new Neo4jClientCommands( this.url, this.primaryKeyProperty );
            this.commands.init();

            if ( this.clear )
            {
                log.info( "Clearing database" );
                this.commands.clearDb();
            }

            log.info( "Initialization complete" );
        }
        catch ( ClientHandlerException che )
        {
            log.error( "Could not connect to server: " + this.url, che );
        }
        catch ( Exception e )
        {
            log.error( "Could not initialize Neo4j database client", e );
        }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException
    {
        try
        {
            this.commands.cleanUp();
        }
        catch ( Exception e )
        {
            log.error( "Error encountered during cleanup", e );
        }
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
        // TODO remove
        // log.debug( "READ: " + key );

        try
        {
            result = this.commands.read( table, key, fields );
            return 0;
        }
        catch ( Exception e )
        {
            log.debug( "Error in READ", e );
        }
        return 1;
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * 
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set
     *            field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan( String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result )
    {
        try
        {
            result = this.commands.scan( table, startkey, recordcount, fields );
            return 0;
        }
        catch ( Exception e )
        {
            log.debug( "Error in SCAN", e );
        }
        return 1;
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
        try
        {
            this.commands.update( table, key, values );
            return 0;
        }
        catch ( Exception e )
        {
            log.debug( "Error in UPDATE", e );
        }
        return 1;
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
        // TODO remove
        // log.debug( "INSERT: " + key );

        try
        {
            this.commands.insert( table, key, values );
            return 0;
        }
        catch ( Exception e )
        {
            log.debug( "Error in INSERT", e );
        }
        return 1;
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
        try
        {
            this.commands.delete( table, key );
            return 0;
        }
        catch ( Exception e )
        {
            log.debug( "Error in DELETE", e );
        }
        return 1;
    }

    // TODO use something like this to construct UPDATE Cypher queries
    // May use later as helper to construct Cypher queries
    private String asDelimitedString( Iterable<String> strings, String prefix )
    {
        StringBuilder sb = new StringBuilder();
        for ( String s : strings )
        {
            sb.append( prefix );
            sb.append( s );
            sb.append( "," );
        }
        return sb.toString().substring( 0, sb.toString().length() - 1 );
    }

}
