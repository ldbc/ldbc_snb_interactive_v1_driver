package com.ldbc.db.neo4j;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Vector;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import OLD_com.ldbc.DBException;

import com.ldbc.data.ByteIterator;
import com.ldbc.data.StringByteIterator;

public class Neo4jClientCommandsRest implements Neo4jClientCommands
{
    private final String url;
    private final String autoIndexKey;

    private RestCypherQueryEngine queryEngine;
    private RestAPI restAPI;

    public Neo4jClientCommandsRest( String url, String autoIndexKey )
    {
        this.url = url;
        this.autoIndexKey = autoIndexKey;
    }

    public void init()
    {
        this.restAPI = new RestGraphDatabase( this.url ).getRestAPI();
        this.queryEngine = new RestCypherQueryEngine( this.restAPI );

        AutoIndexer<Node> nodeAutoIndexer = this.restAPI.index().getNodeAutoIndexer();
        nodeAutoIndexer.setEnabled( true );
        nodeAutoIndexer.startAutoIndexingProperty( this.autoIndexKey );
    }

    public void cleanUp()
    {
        this.restAPI.close();
    }

    public HashMap<String, ByteIterator> read( String table, String key, Set<String> fields ) throws DBException
    {
        // TODO use "table" when 2.0 is released
        final String queryString = String.format( "START n=node:node_auto_index(%s={key}) RETURN n", this.autoIndexKey );

        Node resultNode = null;
        try
        {
            resultNode = (Node) this.queryEngine.query( queryString, MapUtil.map( "key", key ) ).to( Node.class ).single();
        }
        catch ( NoSuchElementException nsee )
        {
            throw new DBException( "Key not found", nsee.getCause() );
        }

        final Iterable<String> fieldsToReturn = ( null == fields ) ? resultNode.getPropertyKeys() : fields;

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();

        for ( String field : fieldsToReturn )
        {
            if ( this.autoIndexKey.equals( field ) )
                result.put( field, new StringByteIterator( (String) resultNode.getProperty( field ) ) );
            else
                // TODO kept separate, likely byte[] when remoting updated
                result.put( field, new StringByteIterator( (String) resultNode.getProperty( field ) ) );
        }

        return result;
    }

    public Vector<Map<String, ByteIterator>> scan( String table, String startkey, int recordcount, Set<String> fields )
    {
        throw new UnsupportedOperationException( "SCAN not supported" );
    }

    public void update( String table, String key, Map<String, ByteIterator> values )
    {
        final String cypherPropertiesString = Neo4jClientUtils.toCypherPropertiesString( values, "n" );
        final Map<String, Object> cypherMap = Neo4jClientUtils.toStringObjectMap( values );
        final String queryString = String.format( "START n=node:node_auto_index(%s={key}) SET %s", this.autoIndexKey,
                cypherPropertiesString );
        cypherMap.put( "key", key );
        this.queryEngine.query( queryString, cypherMap );

    }

    public void insert( String table, String key, Map<String, ByteIterator> values )
    {
        // // TODO use "table" when 2.0 is released
        final Map<String, Object> cypherMap = Neo4jClientUtils.toStringObjectMap( values );
        cypherMap.put( this.autoIndexKey, key );
        final String queryString = "CREATE n = {properties}";
        this.queryEngine.query( queryString, MapUtil.map( "properties", cypherMap ) );
    }

    public void delete( String table, String key )
    {
        // TODO use "table" when 2.0 is released
        final String queryString = String.format( "START n=node:node_auto_index(%s={key}) DELETE n", this.autoIndexKey );
        // TODO use result from DELETE when client provides it (does not now)
        this.queryEngine.query( queryString, MapUtil.map( "key", key ) );
    }

    public void clearDb()
    {
        this.queryEngine.query( "START r=rel(*) DELETE r", MapUtil.map() );
        this.queryEngine.query( "START n=node(*) DELETE n", MapUtil.map() );
    }

    public long nodeCount()
    {
        return this.queryEngine.query( "START n=node(*) RETURN count(n) AS count", MapUtil.map() ).to( Integer.class ).single();
    }

    public long relationshipCount()
    {
        return this.queryEngine.query( "START r=rel(*) RETURN count(r) AS count", MapUtil.map() ).to( Integer.class ).single();
    }
}
