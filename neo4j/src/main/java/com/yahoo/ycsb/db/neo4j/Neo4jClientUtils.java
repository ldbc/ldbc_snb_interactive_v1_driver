package com.yahoo.ycsb.db.neo4j;

import java.util.HashMap;
import java.util.Map;

import com.yahoo.ycsb.util.ByteIterator;

public class Neo4jClientUtils
{
    public static String toCypherPropertiesString( Map<String, ByteIterator> values, String nodeName )
    {
        StringBuilder sb = new StringBuilder();
        for ( String key : values.keySet() )
        {
            sb.append( nodeName );
            sb.append( "." );
            sb.append( key );
            sb.append( "={" );
            sb.append( key );
            sb.append( "}," );
        }
        return sb.toString().substring( 0, sb.toString().length() - 1 );

    }

    public static Map<String, Object> toStringObjectMap( Map<String, ByteIterator> values )
    {
        Map<String, Object> cypherMap = new HashMap<String, Object>();
        for ( String key : values.keySet() )
        {
            cypherMap.put( key, values.get( key ).toString() );
        }
        return cypherMap;
    }

}
