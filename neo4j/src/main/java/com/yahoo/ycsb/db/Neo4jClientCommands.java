package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;

public class Neo4jClientCommands
{
    private final String url;
    private final String autoIndexKey;

    private RestCypherQueryEngine queryEngine;
    private RestAPI restAPI;

    public Neo4jClientCommands( String url, String autoIndexKey )
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

    public Map<String, ByteIterator> read( String table, String key, Set<String> fields )
    {
        // TODO use "table" when 2.0 is released
        final String queryString = String.format( "START n=node:node_auto_index(%s={key}) RETURN n", this.autoIndexKey );
        final Node resultNode = (Node) this.queryEngine.query( queryString, MapUtil.map( "key", key ) ).to( Node.class ).single();

        final Iterable<String> fieldsToReturn = ( null == fields ) ? resultNode.getPropertyKeys() : fields;

        Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();

        for ( String field : fieldsToReturn )
        {
            if ( this.autoIndexKey.equals( field ) )
                result.put( field, ByteArrayByteIterator.fromString( (String) resultNode.getProperty( field ) ) );
            else
                // TODO kept separate, likely byte[] when remoting updated
                result.put( field, ByteArrayByteIterator.fromString( (String) resultNode.getProperty( field ) ) );
        }

        return result;
    }

    public Vector<Map<String, ByteIterator>> scan( String table, String startkey, int recordcount, Set<String> fields )
    {
        throw new UnsupportedOperationException( "SCAN not supported" );
    }

    public void update( String table, String key, Map<String, ByteIterator> values )
    {
        Map<String, Object> neo4jValues = new HashMap<String, Object>();
        for ( String valueKey : values.keySet() )
        {
            neo4jValues.put( valueKey, values.get( valueKey ).toString() );
        }

        // TODO use "table" when 2.0 is released
        // TODO FIX! this overwrites ALL PROPERTIES (deletes some)
        neo4jValues.put( this.autoIndexKey, key );
        final String queryString = String.format( "START n=node:node_auto_index(%s={key}) SET n={properties}",
                this.autoIndexKey );
        this.queryEngine.query( queryString, MapUtil.map( "key", key, "properties", neo4jValues ) );
    }

    public void insert( String table, String key, Map<String, ByteIterator> values )
    {
        Map<String, Object> neo4jValues = new HashMap<String, Object>();
        for ( String valueKey : values.keySet() )
        {
            neo4jValues.put( valueKey, values.get( valueKey ).toString() );
        }

        // TODO use "table" when 2.0 is released
        neo4jValues.put( this.autoIndexKey, key );
        final String queryString = "CREATE n = {properties}";
        this.queryEngine.query( queryString, MapUtil.map( "properties", neo4jValues ) );
    }

    public void clearDb()
    {
        this.queryEngine.query( "START n=node(*) DELETE n", MapUtil.map() );
        this.queryEngine.query( "START r=rel(*) DELETE r", MapUtil.map() );
    }

    public int nodeCount()
    {
        return this.queryEngine.query( "START n=node(*) RETURN count(n)", MapUtil.map() ).to( Integer.class ).single();
    }

    public int relationshipCount()
    {
        return this.queryEngine.query( "START r=rel(*) RETURN count(r)", MapUtil.map() ).to( Integer.class ).single();
    }

}
