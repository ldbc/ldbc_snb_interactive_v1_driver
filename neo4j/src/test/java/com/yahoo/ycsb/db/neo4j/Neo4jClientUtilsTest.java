package com.yahoo.ycsb.db.neo4j;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.yahoo.ycsb.db.neo4j.Neo4jClientUtils;
import com.yahoo.ycsb.util.ByteIterator;
import com.yahoo.ycsb.util.StringByteIterator;

public class Neo4jClientUtilsTest
{
    @Test
    public void toCypherPropertiesStringTest()
    {
        // Given
        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        values.put( "prop1", new StringByteIterator( "val1" ) );
        values.put( "prop2", new StringByteIterator( "val2" ) );
        String nodeName = "n";

        // When
        String propsString = Neo4jClientUtils.toCypherPropertiesString( values, nodeName );

        // Then
        Set<String> permutations = new HashSet<String>();
        permutations.add( "n.prop1={prop1},n.prop2={prop2}" );
        permutations.add( "n.prop2={prop2},n.prop1={prop1}" );
        assertEquals( true, permutations.contains( propsString ) );
    }

    @Test
    public void toStringObjectMap()
    {
        // Given
        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        values.put( "prop1", new StringByteIterator( "val1" ) );
        values.put( "prop2", new StringByteIterator( "val2" ) );

        // When
        Map<String, Object> map = Neo4jClientUtils.toStringObjectMap( values );

        // Then
        assertEquals( "val1", map.get( "prop1" ) );
        assertEquals( "val2", map.get( "prop2" ) );
    }

}
