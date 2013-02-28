package com.yahoo.ycsb.db;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

@Ignore
public class Neo4jClientCommandsTest
{
    private final String TABLE = "_neo4j_usertable";
    private final String PRIMARY_KEY = "_neo4j_primary_key";
    private final String NEO4J_SERVER_URL = "http://localhost:7474/db/data";
    private Neo4jClientCommands commands;

    @Before
    public void init()
    {
        this.commands = new Neo4jClientCommands( NEO4J_SERVER_URL, PRIMARY_KEY );
        this.commands.init();
        this.commands.clearDb();
        assertEquals( "Database should contain zero nodes", 0, commands.nodeCount() );
        doPopulate();
        assertEquals( "Database should contain two nodes", 3, commands.nodeCount() );
    }

    @Test
    public void insert()
    {
        assertEquals( "Database should contain two nodes", 3, commands.nodeCount() );

        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        values.put( "name", ByteArrayByteIterator.fromString( "nico" ) );
        values.put( "age", ByteArrayByteIterator.fromString( "26" ) );
        this.commands.insert( TABLE, "4", values );

        assertEquals( "Database should contain three nodes", 4, commands.nodeCount() );
    }

    @Test
    public void readNonExistentNode() throws DBException
    {
        assertNodeDoesNotExist( "99" );
    }

    @Test
    public void readAllFields() throws DBException
    {
        Map<String, ByteIterator> result = commands.read( TABLE, "1", null );
        result.remove( PRIMARY_KEY );
        assertEquals( 3, result.size() );
        assertEquals( "alex", result.get( "name" ).toString() );
        assertEquals( "31", result.get( "age" ).toString() );
        assertEquals( "nz", result.get( "country" ).toString() );
    }

    @Test
    public void readSomeFields() throws DBException
    {
        Set<String> values = new HashSet<String>();
        values.add( "age" );
        Map<String, ByteIterator> result = commands.read( TABLE, "1", values );
        result.remove( PRIMARY_KEY );
        assertEquals( 1, result.size() );
        assertEquals( "31", result.get( "age" ).toString() );
    }

    @Test
    public void update() throws DBException
    {
        Map<String, ByteIterator> result = commands.read( TABLE, "2", null );
        assertEquals( "jake", result.get( "name" ).toString() );
        assertEquals( "25", result.get( "age" ).toString() );
        assertEquals( "se", result.get( "country" ).toString() );

        Map<String, ByteIterator> writeValues = new HashMap<String, ByteIterator>();
        writeValues.put( "country", ByteArrayByteIterator.fromString( "sweden" ) );
        writeValues.put( "name", ByteArrayByteIterator.fromString( "jacob" ) );
        // TODO remove "age" line. Cypher SET deletes fields
        writeValues.put( "age", ByteArrayByteIterator.fromString( "25" ) );

        commands.update( TABLE, "2", writeValues );

        result = commands.read( TABLE, "2", null );
        assertEquals( "jacob", result.get( "name" ).toString() );
        assertEquals( "25", result.get( "age" ).toString() );
        assertEquals( "sweden", result.get( "country" ).toString() );
    }

    @Test
    public void delete() throws DBException
    {
        Map<String, ByteIterator> result = commands.read( TABLE, "3", null );
        assertEquals( "temp guy", result.get( "name" ).toString() );

        commands.delete( TABLE, "3" );

        assertNodeDoesNotExist( "3" );
    }

    @Ignore
    @Test
    public void deleteNonExistentNode() throws DBException
    {
        assertEquals( false, true );
    }

    @Ignore
    @Test
    public void scan()
    {
        assertEquals( false, true );
    }

    @Test
    public void byteArrayByteIteratorTest()
    {
        assertEquals( "hello", new String( new ByteArrayByteIterator( "hello".getBytes() ).toArray() ) );
        assertEquals( "hello", ByteArrayByteIterator.fromString( "hello" ).toString() );
        assertEquals( true,
                Arrays.equals( new ByteArrayByteIterator( new byte[] { 1, 2 } ).toArray(), new byte[] { 1, 2 } ) );
    }

    @Test
    public void stringByteIteratorTest()
    {
        assertEquals( "hello", new StringByteIterator( "hello" ).toString() );
    }

    private void doPopulate()
    {
        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        values.put( "name", ByteArrayByteIterator.fromString( "alex" ) );
        values.put( "age", ByteArrayByteIterator.fromString( "31" ) );
        values.put( "country", ByteArrayByteIterator.fromString( "nz" ) );
        this.commands.insert( TABLE, "1", values );

        values = new HashMap<String, ByteIterator>();
        values.put( "name", ByteArrayByteIterator.fromString( "jake" ) );
        values.put( "age", ByteArrayByteIterator.fromString( "25" ) );
        values.put( "country", ByteArrayByteIterator.fromString( "se" ) );
        this.commands.insert( TABLE, "2", values );

        values = new HashMap<String, ByteIterator>();
        values.put( "name", ByteArrayByteIterator.fromString( "temp guy" ) );
        this.commands.insert( TABLE, "3", values );
    }

    private void assertNodeDoesNotExist( String key )
    {
        boolean readSucceeded = true;
        try
        {
            commands.read( TABLE, key, null );
            readSucceeded = true;
        }
        catch ( DBException dbe )
        {
            readSucceeded = false;
        }
        assertEquals( false, readSucceeded );
    }

}
