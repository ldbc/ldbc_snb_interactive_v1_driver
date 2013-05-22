package com.ldbc.db.neo4j;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Vector;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.helpers.collection.MapUtil;

import OLD_com.ldbc.DBException;

import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.data.StringByteIterator;

public class Neo4jClientCommandsEmbedded implements Neo4jClientCommands
{
    private final String path;
    private final String autoIndexKey;

    private ExecutionEngine queryEngine;
    private GraphDatabaseService db;

    public Neo4jClientCommandsEmbedded( String path, String autoIndexKey )
    {
        this.path = path;
        this.autoIndexKey = autoIndexKey;
    }

    public void init()
    {
        this.db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( this.path ).newGraphDatabase();
        this.queryEngine = new ExecutionEngine( this.db );

        AutoIndexer<Node> nodeAutoIndexer = this.db.index().getNodeAutoIndexer();
        nodeAutoIndexer.setEnabled( true );
        nodeAutoIndexer.startAutoIndexingProperty( this.autoIndexKey );

        registerShutdownHook( this.db );
    }

    public void cleanUp()
    {
        this.db.shutdown();
    }

    public HashMap<String, ByteIterator> read( String table, String key, Set<String> fields ) throws DBException
    {
        // TODO use "table" when 2.0 is released
        final String queryString = String.format( "START n=node:node_auto_index(%s={key}) RETURN n", this.autoIndexKey );

        Node resultNode = null;
        try
        {
            // TODO [extract] only line that needed changing from REST impl
            resultNode = (Node) this.queryEngine.execute( queryString, MapUtil.map( "key", key ) ).columnAs( "n" ).next();
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
        // TODO [extract] only line that needed changing from REST impl
        this.queryEngine.execute( queryString, cypherMap );
    }

    public void insert( String table, String key, Map<String, ByteIterator> values )
    {
        // TODO use "table" when 2.0 is released
        final Map<String, Object> cypherMap = Neo4jClientUtils.toStringObjectMap( values );
        cypherMap.put( this.autoIndexKey, key );
        final String queryString = "CREATE n = {properties}";
        // TODO [extract] only line that needed changing from REST impl
        this.queryEngine.execute( queryString, MapUtil.map( "properties", cypherMap ) );
    }

    public void delete( String table, String key ) throws DBException
    {
        // TODO use "table" when 2.0 is released
        final String queryString = String.format( "START n=node:node_auto_index(%s={key}) DELETE n", this.autoIndexKey );
        // TODO [extract] only line that needed changing from REST impl
        int deletedNodes = this.queryEngine.execute( queryString, MapUtil.map( "key", key ) ).getQueryStatistics().getDeletedNodes();
        if ( deletedNodes != 1 ) throw new DBException( String.format( "%s nodes deleted expected 1", deletedNodes ) );
    }

    public void clearDb()
    {
        // TODO [extract] only lines that needed changing from REST impl
        this.queryEngine.execute( "START r=rel(*) DELETE r", MapUtil.map() );
        this.queryEngine.execute( "START n=node(*) DELETE n", MapUtil.map() );
    }

    public long nodeCount()
    {
        // TODO [extract] only lines that needed changing from REST impl
        return (Long) this.queryEngine.execute( "START n=node(*) RETURN count(n) AS count", MapUtil.map() ).columnAs(
                "count" ).next();
    }

    public long relationshipCount()
    {
        // TODO [extract] only lines that needed changing from REST impl
        return (Long) this.queryEngine.execute( "START r=rel(*) RETURN count(r) AS count", MapUtil.map() ).columnAs(
                "count" ).next();
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }

}
