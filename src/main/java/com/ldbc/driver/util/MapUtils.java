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
            sb.append( prefix ).append( entry.getKey().toString() ).append( " = " ).append( entry.getValue().toString() ).append(
                    "\n" );
        }
        return sb.toString();
    }

    /**
     * Returns new Map
     * 
     * @param map
     * @param excludedKeys
     * @return
     */
    public static <K, V> Map<K, V> copyExcludingKeys( Map<K, V> map, Set<K> excludedKeys )
    {
        Map<K, V> resultMap = new HashMap<K, V>();
        for ( Entry<K, V> entry : map.entrySet() )
        {
            if ( false == excludedKeys.contains( entry.getKey() ) ) resultMap.put( entry.getKey(), entry.getValue() );
        }
        return resultMap;
    }

    public static <K, V> V getDefault( Map<K, V> map, K key, V defaultValue )
    {
        return ( map.containsKey( key ) ) ? map.get( key ) : defaultValue;
    }

    /**
     * Returns new Map
     * 
     * @param properties
     * @param map
     * @param overwrite
     * @return
     */
    public static <K, V> Map<K, V> mergePropertiesToMap( Properties properties, Map<K, V> map, boolean overwrite )
    {
        Map<K, V> resultMap = new HashMap<K, V>();
        for ( K mapKey : map.keySet() )
        {
            resultMap.put( mapKey, map.get( mapKey ) );
        }
        for ( Object propertyKey : properties.keySet() )
        {
            if ( ( overwrite ) || ( false == resultMap.containsKey( (K) propertyKey ) ) )
            {
                resultMap.put( (K) propertyKey, (V) properties.get( (K) propertyKey ) );
            }
        }
        return resultMap;
    }

    /**
     * Returns new Properties
     * 
     * @param map
     * @param properties
     * @param overwrite
     * @return
     */
    public static <K, V> Properties mergeMapToProperties( Map<K, V> map, Properties properties, boolean overwrite )
    {
        Properties resultProperties = new Properties();
        for ( Object propertyKey : properties.keySet() )
        {
            resultProperties.put( propertyKey, properties.get( propertyKey ) );
        }
        for ( K mapKey : map.keySet() )
        {
            if ( ( overwrite ) || ( false == resultProperties.containsKey( mapKey ) ) )
            {
                resultProperties.put( mapKey, map.get( mapKey ) );
            }
        }
        return resultProperties;
    }

    /**
     * map1 value overwrites map2 value if they share a common key
     * 
     * @param map1
     * @param map2
     * @param mergeFun
     * @return
     */
    public static <K, V> Map<K, V> mergeMaps( Map<K, V> map1, Map<K, V> map2, final boolean overwrite )
    {
        Function2<V, V, V> overwriteFun = new Function2<V, V, V>()
        {
            @Override
            public V apply( V from1, V from2 )
            {
                return ( overwrite ) ? from1 : from2;
            }
        };
        return mergeMaps( map1, map2, overwriteFun );
    }

    /**
     * mergeFun only called for keys that exist in both Map instances
     * 
     * @param map1
     * @param map2
     * @param mergeFun
     * @return
     */
    public static <K, V> Map<K, V> mergeMaps( Map<K, V> map1, Map<K, V> map2, Function2<V, V, V> mergeFun )
    {
        Map<K, V> resultMap = new HashMap<K, V>();
        for ( K map1Key : map1.keySet() )
        {
            resultMap.put( map1Key, map1.get( map1Key ) );
        }
        for ( K map2Key : map2.keySet() )
        {
            if ( resultMap.containsKey( map2Key ) )
            {
                resultMap.put( map2Key, mergeFun.apply( map1.get( map2Key ), map2.get( map2Key ) ) );
            }
            else
            {
                resultMap.put( map2Key, map2.get( map2Key ) );
            }
        }
        return resultMap;
    }
}
