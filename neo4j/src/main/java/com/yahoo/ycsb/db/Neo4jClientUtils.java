package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;

import com.yahoo.ycsb.ByteIterator;

public class Neo4jClientUtils
{
    // TODO test
    public static Map<String, Object> toStringObjectMap( Map<String, ByteIterator> values, String indexKey,
            String indexValue )
    {
        Map<String, Object> cypherParams = new HashMap<String, Object>();
        for ( String valueKey : values.keySet() )
        {
            cypherParams.put( valueKey, values.get( valueKey ).toString() );
        }
        // TODO use "table" when 2.0 is released
        cypherParams.put( indexKey, indexValue );
        return cypherParams;
    }

    // TODO test
    public static String toCypherPropertiesString( Map<String, ByteIterator> values, String nodeName )
    {
        StringBuilder sb = new StringBuilder();
        for ( String valueKey : values.keySet() )
        {
            sb.append( nodeName );
            sb.append( "." );
            sb.append( valueKey );
            sb.append( "='" );
            sb.append( values.get( valueKey ).toString() );
            sb.append( "'," );
        }
        return sb.toString().substring( 0, sb.toString().length() - 1 );
    }

}
