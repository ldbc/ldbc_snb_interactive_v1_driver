/**
 * Neo4j client binding for YCSB.
 *
 * Submitted by Alex Averbuch on 25/02/2013.
 *
 */

package com.yahoo.ycsb.db.neo4j;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.sun.jersey.api.client.ClientHandlerException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Utils;

/**
 * Neo4j client for YCSB framework.
 * 
 * Properties to set:
 * 
 * neo4j.url=http://localhost:7474/db/data <br>
 * neo4j.primarykey=primarykey <br>
 * neo4j.table=usertable <br>
 * neo4j.clear=false <br>
 * neo4j.path=/tmp/db <br>
 * neo4j.dbtype=embedded <br>
 * 
 * @author Alex Averbuch
 */
public class Neo4jClient extends DB
{
    private Neo4jClientCommands commands;

    private String url;
    private String primaryKeyProperty;
    private boolean clear;
    private String dbtype;
    private String path;
    // TODO use "table" when 2.0 is released
    private String table;

    private static Logger logger = Logger.getLogger( Neo4jClient.class );

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
            url = Utils.mapGetDefault( getProperties(), "neo4j.url", "http://localhost:7474/db/data" );
            primaryKeyProperty = Utils.mapGetDefault( getProperties(), "neo4j.primarykey", "primarykey" );
            // TODO use "table" when 2.0 is released
            table = Utils.mapGetDefault( getProperties(), "neo4j.table", "usertable" );
            clear = Boolean.parseBoolean( Utils.mapGetDefault( getProperties(), "neo4j.clear", "false" ) );
            path = Utils.mapGetDefault( getProperties(), "neo4j.path", "/tmp/db" );
            dbtype = Utils.mapGetDefault( getProperties(), "neo4j.dbtype", "embedded" );

            logger.info( "*** Neo4j Properties ***" );
            logger.info( "table = " + table );
            logger.info( "primary key = " + primaryKeyProperty );
            logger.info( "clear database = " + clear );
            logger.info( "database type = " + dbtype );
            logger.info( "url = " + url );
            logger.info( "path = " + path );
            logger.info( "************************" );

            if ( dbtype.equals( "server" ) )
            {
                logger.info( "Connecting to database: " + url );
                commands = new Neo4jClientCommandsRest( url, primaryKeyProperty );
            }
            else if ( dbtype.equals( "embedded" ) )
            {
                logger.info( "Connecting to database: " + path );
                commands = new Neo4jClientCommandsEmbedded( path, primaryKeyProperty );
            }
            else
            {
                logger.error( String.format( "Invalid database type: %s. Must be 'server' or 'embedded'", dbtype ) );
            }

            commands.init();

            if ( clear )
            {
                logger.info( "Clearing database" );
                commands.clearDb();
            }

            logger.info( "Initialization complete" );
        }
        // TODO (1) can never happen? (2) should be caught in REST client?
        catch ( ClientHandlerException che )
        {
            String msg = "Could not connect to server: " + url;
            logger.error( msg, che.getCause() );
            throw new DBException( msg, che.getCause() );
        }
        catch ( Exception e )
        {
            String msg = "Could not initialize Neo4j database client";
            logger.error( msg, e.getCause() );
            throw new DBException( msg, e.getCause() );
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
            commands.cleanUp();
        }
        catch ( Exception e )
        {
            String msg = "Error encountered during cleanup";
            logger.error( msg, e.getCause() );
            throw new DBException( msg, e.getCause() );
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
        try
        {
            result = commands.read( table, key, fields );
            return 0;
        }
        catch ( Exception e )
        {
            logger.debug( "Error in READ", e.getCause() );
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
            result = commands.scan( table, startkey, recordcount, fields );
            return 0;
        }
        catch ( Exception e )
        {
            logger.debug( "Error in SCAN", e.getCause() );
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
            commands.update( table, key, values );
            return 0;
        }
        catch ( Exception e )
        {
            logger.debug( "Error in UPDATE", e.getCause() );
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
        try
        {
            commands.insert( table, key, values );
            return 0;
        }
        catch ( Exception e )
        {
            logger.debug( "Error in INSERT", e.getCause() );
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
            commands.delete( table, key );
            return 0;
        }
        catch ( Exception e )
        {
            logger.debug( "Error in DELETE", e.getCause() );
        }
        return 1;
    }
}
