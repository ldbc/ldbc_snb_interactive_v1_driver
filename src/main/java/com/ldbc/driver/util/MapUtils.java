package com.ldbc.driver.util;

import com.google.common.collect.Lists;

import java.util.*;
import java.util.Map.Entry;

public class MapUtils {
    public static <K, V> String prettyPrint(Map<K, V> map) {
        return prettyPrint(map, "");
    }

    public static <K, V> String prettyPrint(Map<K, V> map, String prefix) {
        List<Entry<K, V>> mapEntries = sortedEntrySet(map);
        StringBuilder sb = new StringBuilder();
        for (Entry<K, V> entry : mapEntries) {
            String keyString = (null == entry.getKey()) ? "null" : entry.getKey().toString();
            String valueString = (null == entry.getValue()) ? "null" : entry.getValue().toString();
            sb.append(prefix).append(keyString).append(" = ").append(valueString).append("\n");
        }
        return sb.toString();
    }

    public static <K, V> List<Entry<K, V>> sortedEntrySet(Map<K, V> map) {
        return sortedEntries(map.entrySet());
    }

    public static <K, V> List<Entry<K, V>> sortedEntries(Iterable<Entry<K, V>> entries) {
        List<Entry<K, V>> sortedEntries = Lists.newArrayList(entries);
        Collections.sort(sortedEntries, new EntriesComparator<K>());
        return sortedEntries;
    }

    private static class EntriesComparator<K> implements Comparator<Entry<K, ?>> {
        @Override
        public int compare(Entry<K, ?> o1, Entry<K, ?> o2) {
            if (o1.getKey() instanceof Comparable)
                return ((Comparable) o1.getKey()).compareTo(o2.getKey());
            else
                return 0;
        }
    }

    /**
     * Returns new Map
     *
     * @param map
     * @param excludedKeys
     * @return
     */
    public static <K, V> Map<K, V> copyExcludingKeys(Map<K, V> map, Set<K> excludedKeys) {
        Map<K, V> resultMap = new HashMap<>();
        for (Entry<K, V> entry : map.entrySet()) {
            if (false == excludedKeys.contains(entry.getKey())) resultMap.put(entry.getKey(), entry.getValue());
        }
        return resultMap;
    }

    public static <K, V> V getDefault(Map<K, V> map, K key, V defaultValue) {
        return (map.containsKey(key)) ? map.get(key) : defaultValue;
    }

    public static <K, V> Map<K, V> propertiesToMap(Properties properties) {
        Map<K, V> resultMap = new HashMap<>();
        for (Object propertyKey : properties.keySet()) {
            resultMap.put((K) propertyKey, (V) properties.get((K) propertyKey));
        }
        return resultMap;
    }

    /**
     * Returns new Map
     *
     * @param properties
     * @param map
     * @param overwrite
     * @return
     */
    public static <K, V> Map<K, V> mergePropertiesToMap(Properties properties, Map<K, V> map, boolean overwrite) {
        Map<K, V> resultMap = new HashMap<>();
        for (K mapKey : map.keySet()) {
            resultMap.put(mapKey, map.get(mapKey));
        }
        for (Object propertyKey : properties.keySet()) {
            if ((overwrite) || (false == resultMap.containsKey((K) propertyKey))) {
                resultMap.put((K) propertyKey, (V) properties.get((K) propertyKey));
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
    public static <K, V> Properties mergeMapToProperties(Map<K, V> map, Properties properties, boolean overwrite) {
        Properties resultProperties = new Properties();
        for (Object propertyKey : properties.keySet()) {
            resultProperties.put(propertyKey, properties.get(propertyKey));
        }
        for (K mapKey : map.keySet()) {
            if ((overwrite) || (false == resultProperties.containsKey(mapKey))) {
                resultProperties.put(mapKey, map.get(mapKey));
            }
        }
        return resultProperties;
    }

    /**
     * newMap value overwrites originalMap value if they share a common key
     *
     * @param originalMap
     * @param newMap
     * @param overwrite
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> Map<K, V> mergeMaps(Map<K, V> originalMap, Map<K, V> newMap, final boolean overwrite) {
        Function2<V, V, V> overwriteFun = new Function2<V, V, V>() {
            @Override
            public V apply(V originalVal, V newVal) {
                return (overwrite) ? newVal : originalVal;
            }
        };
        return mergeMaps(originalMap, newMap, overwriteFun);
    }

    /**
     * mergeFun only called for keys that exist in both Map instances
     *
     * @param originalMap
     * @param newMap
     * @param mergeFun
     * @return
     */
    public static <K, V> Map<K, V> mergeMaps(Map<K, V> originalMap, Map<K, V> newMap, Function2<V, V, V> mergeFun) {
        Map<K, V> resultMap = new HashMap<>();
        for (K originalMapKey : originalMap.keySet()) {
            resultMap.put(originalMapKey, originalMap.get(originalMapKey));
        }
        for (K newMapKey : newMap.keySet()) {
            if (resultMap.containsKey(newMapKey)) {
                resultMap.put(newMapKey, mergeFun.apply(originalMap.get(newMapKey), newMap.get(newMapKey)));
            } else {
                resultMap.put(newMapKey, newMap.get(newMapKey));
            }
        }
        return resultMap;
    }
}
