/**
 * Neo4j client binding for YCSB.
 *
 * Submitted by Alex Averbuch on 25/02/2013.
 *
 */

package com.yahoo.ycsb.db;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import com.sun.jersey.api.client.ClientHandlerException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Neo4j client for YCSB framework.
 * 
 * Properties to set:
 * 
 * neo4j.url=http://localhost:7474/db/data <br>
 * neo4j.primarykey=primarykey <br>
 * neo4j.table=usertable <br>
 * 
 * @author Alex Averbuch
 */
public class Neo4jClient extends DB
{
    private RestCypherQueryEngine queryEngine;
    private RestAPI restAPI;

    private String url;
    private String primaryKeyProperty;
    // TODO use "table" when 2.0 is released
    private String table;

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

            System.out.println( "Neo4j \"primary key\" = " + this.primaryKeyProperty );
            System.out.println( "Neo4j database url = " + this.url );
            System.out.println( "Neo4j connecting to " + this.url );

            // Connect to database server
            this.restAPI = new RestGraphDatabase( url ).getRestAPI();
            this.queryEngine = new RestCypherQueryEngine( this.restAPI );

            System.out.println( "Neo4j clearing database" );

            // Clear DB
            this.queryEngine.query( "START r=rel(*) DELETE r", MapUtil.map() );
            this.queryEngine.query( "START n=node(*) DELETE n", MapUtil.map() );

            System.out.println( "Neo4j configuring autoindexing" );

            // Configure indexes
            AutoIndexer<Node> nodeAutoIndexer = this.restAPI.index().getNodeAutoIndexer();
            nodeAutoIndexer.setEnabled( true );
            nodeAutoIndexer.startAutoIndexingProperty( this.primaryKeyProperty );

            System.out.println( "Neo4j initialization complete" );
        }
        catch ( ClientHandlerException che )
        {
            String errString = "[Neo4jClient] Could not connect to server: " + this.url;
            System.err.println( errString + "\n" + che.toString() );
            throw new DBException( errString, che.getCause() );

        }
        catch ( Exception e )
        {
            String errString = "[Neo4jClient] Could not initialize Neo4j database client";
            System.err.println( errString + "\n" + e.toString() );
            throw new DBException( errString, e.getCause() );
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
            super.cleanup();
            this.restAPI.close();
        }
        catch ( Exception e )
        {
            String errString = "[Neo4jClient] Error encountered during cleanup";
            System.err.println( errString + "\n" + e.toString() );
            throw new DBException( errString, e.getCause() );
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
            // TODO use "table" when 2.0 is released
            final String queryString = String.format( "START n=node:node_auto_index(%s={key}) RETURN n",
                    this.primaryKeyProperty );
            final Node resultNode = (Node) this.queryEngine.query( queryString, MapUtil.map( "key", key ) ).to(
                    Node.class ).single();

            final Iterable<String> fieldsToReturn = ( null == fields ) ? resultNode.getPropertyKeys() : fields;

            for ( String field : fieldsToReturn )
                result.put( field, new StringByteIterator( (String) resultNode.getProperty( field ) ) );

            return 0;
        }
        catch ( Exception e )
        {
            String errString = "[Neo4jClient] Error in READ";
            System.err.println( errString + "\n" + e.toString() );
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
        System.out.println( "SCAN not supported" );
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
            HashMap<String, String> stringValues = StringByteIterator.getStringMap( values );

            // TODO use "table" when 2.0 is released
            final String queryString = String.format( "START n=node:node_auto_index(%s={key}) SET n={properties}",
                    this.primaryKeyProperty );
            this.queryEngine.query( queryString, MapUtil.map( "key", key, "properties", stringValues ) );

            return 0;
        }
        catch ( Exception e )
        {
            String errString = "[Neo4jClient] Error in UPDATE";
            System.err.println( errString + "\n" + e.toString() );
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
            HashMap<String, String> stringValues = StringByteIterator.getStringMap( values );

            // TODO use "table" when 2.0 is released
            stringValues.put( this.primaryKeyProperty, key );
            final String queryString = "CREATE n = {properties}";
            this.queryEngine.query( queryString, MapUtil.map( "properties", stringValues ) );

            return 0;
        }
        catch ( Exception e )
        {
            String errString = "[Neo4jClient] Error in INSERT";
            System.err.println( errString + "\n" + e.toString() );
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
            // TODO use "table" when 2.0 is released
            final String queryString = String.format( "START n=node:node_auto_index(%s={key}) DELETE n",
                    this.primaryKeyProperty );
            final int nodesDeleted = this.queryEngine.query( queryString, MapUtil.map( "key", key ) ).to( Integer.class ).single();

            if ( nodesDeleted == 1 )
                return 0;
            else
                throw new DBException( String.format( "%s nodes deleted, 1 expected", nodesDeleted ) );
        }
        catch ( Exception e )
        {
            String errString = "[Neo4jClient] Error in DELETE";
            System.err.println( errString + "\n" + e.toString() );
        }

        return 1;
    }

    private void error( String msg, Exception e ) throws DBException
    {
        debug( msg, e );
        throw new DBException( msg, e.getCause() );
    }

    private void debug( String msg, Exception e )
    {
        msg = "[Neo4jClient] " + msg;
        System.err.println( msg + "\n" + exceptionToString( e ) );
    }

    private void info( String msg, Exception e )
    {
        msg = "[Neo4jClient] " + msg;
        System.out.println( msg + "\n" + exceptionToString( e ) );
    }

    private String exceptionToString( Exception e )
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        e.printStackTrace( pw );
        return sw.toString();
    }

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
