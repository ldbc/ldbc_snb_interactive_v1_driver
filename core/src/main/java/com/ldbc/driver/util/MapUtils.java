package com.ldbc.driver.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

public class MapUtils
{
    public static <K, V> String prettyPrint( Map<K, V> map )
    {
        return prettyPrint( map, "" );
    }

    public static <K, V> String prettyPrint( Map<K, V> map, String prefix )
    {
        StringBuilder sb = new StringBuilder();
        for ( Entry<K, V> entry : map.entrySet() )
        {
            sb.append( prefix ).append( entry.getKey() ).append( " = " ).append( entry.getValue() ).append( "\n" );
        }
        return sb.toString();
    }

    public static <K, V> Map<K, V> copyExcludingKeys( Map<K, V> map, Set<K> excludedKeys )
    {
        Map<K, V> newMap = new HashMap<K, V>();
        for ( Entry<K, V> entry : map.entrySet() )
        {
            if ( false == excludedKeys.contains( entry.getKey() ) ) newMap.put( entry.getKey(), entry.getValue() );
        }
        return newMap;
    }

    public static <K, V> V getDefault( Map<K, V> map, K key, V defaultValue )
    {
        return ( map.containsKey( key ) ) ? map.get( key ) : defaultValue;
    }

    public static <K, V> Map<K, V> mergePropertiesToMap( Properties fromProperties, Map<K, V> toMap, boolean overwrite )
    {
        for ( Object fromPropertyKey : fromProperties.keySet() )
        {
            if ( ( overwrite ) || ( false == toMap.containsKey( (K) fromPropertyKey ) ) )
            {
                toMap.put( (K) fromPropertyKey, (V) fromProperties.get( (K) fromPropertyKey ) );
            }
        }
        return toMap;
    }

    public static <K, V> Properties mergeMapToProperties( Map<K, V> fromMap, Properties toProperties, boolean overwrite )
    {
        for ( K fromMapKey : fromMap.keySet() )
        {
            if ( ( overwrite ) || ( false == toProperties.containsKey( fromMapKey ) ) )
            {
                toProperties.put( fromMapKey, fromMap.get( fromMapKey ) );
            }
        }
        return toProperties;
    }

    public static <K, V> Map<K, V> mergeMaps( Map<K, V> fromMap, Map<K, V> toMap, boolean overwrite )
    {
        for ( K fromMapKey : fromMap.keySet() )
        {
            if ( ( overwrite ) || ( false == toMap.containsKey( fromMapKey ) ) )
            {
                toMap.put( fromMapKey, fromMap.get( fromMapKey ) );
            }
        }
        return toMap;
    }

}
