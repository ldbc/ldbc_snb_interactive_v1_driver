package com.ldbc.driver.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class MapUtilsTest
{
    @Test
    public void shouldNotCopyExcludedKeys()
    {
        // Given
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        Set<Integer> excludeKey2 = new HashSet<Integer>( Arrays.asList( new Integer[] { 2 } ) );

        // When
        Map<Integer, Integer> map13 = MapUtils.copyExcludingKeys( map123, excludeKey2 );

        // Then
        assertThat( map13.size(), is( 2 ) );
        assertThat( map13.containsKey( 1 ), is( true ) );
        assertThat( map13.containsKey( 2 ), is( false ) );
        assertThat( map13.containsKey( 3 ), is( true ) );
    }

    @Test
    public void shouldGetDefaultWhenKeyNotPresent()
    {
        // Given
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        // When
        int key1Value = MapUtils.getDefault( map123, 1, 9 );
        int key4Value = MapUtils.getDefault( map123, 4, 4 );

        // Then
        assertThat( key1Value, is( 1 ) );
        assertThat( key4Value, is( 4 ) );
    }

    @Test
    public void shouldMergeAllPropertiesToMapWithOverwrite()
    {
        // Given
        Properties properties123 = new Properties();
        properties123.put( 1, 1 );
        properties123.put( 2, 2 );
        properties123.put( 3, 3 );

        // When
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 2 );
        map123 = MapUtils.mergePropertiesToMap( properties123, map123, true );

        // Then
        assertThat( map123.size(), is( 3 ) );
        assertThat( map123.get( 1 ), is( 1 ) );
        assertThat( map123.get( 2 ), is( 2 ) );
        assertThat( map123.get( 3 ), is( 3 ) );
    }

    @Test
    public void shouldMergeAllPropertiesToMapWithoutOverwrite()
    {
        // Given
        Properties properties123 = new Properties();
        properties123.put( 1, 1 );
        properties123.put( 2, 2 );
        properties123.put( 3, 3 );

        // When
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 2 );
        map123 = MapUtils.mergePropertiesToMap( properties123, map123, false );

        // Then
        assertThat( map123.size(), is( 3 ) );
        assertThat( map123.get( 1 ), is( 2 ) );
        assertThat( map123.get( 2 ), is( 2 ) );
        assertThat( map123.get( 3 ), is( 3 ) );
    }

    @Test
    public void shouldMergeAllMapToPropertiesWithOverwrite()
    {
        // Given
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        // When
        Properties properties123 = new Properties();
        properties123.put( 1, 2 );
        properties123 = MapUtils.mergeMapToProperties( map123, properties123, true );

        // Then
        assertThat( properties123.size(), is( 3 ) );
        assertThat( (Integer) properties123.get( 1 ), is( 1 ) );
        assertThat( (Integer) properties123.get( 2 ), is( 2 ) );
        assertThat( (Integer) properties123.get( 3 ), is( 3 ) );
    }

    @Test
    public void shouldMergeAllMapToPropertiesWithoutOverwrite()
    {
        // Given
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        // When
        Properties properties123 = new Properties();
        properties123.put( 1, 2 );
        properties123 = MapUtils.mergeMapToProperties( map123, properties123, false );

        // Then
        assertThat( properties123.size(), is( 3 ) );
        assertThat( (Integer) properties123.get( 1 ), is( 2 ) );
        assertThat( (Integer) properties123.get( 2 ), is( 2 ) );
        assertThat( (Integer) properties123.get( 3 ), is( 3 ) );
    }

    @Test
    public void shouldMergeAllMapToMapWithOverwrite()
    {
        // Given
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        // When
        Map<Integer, Integer> newMap123 = new HashMap<Integer, Integer>();
        newMap123.put( 1, 2 );
        newMap123 = MapUtils.mergeMaps( map123, newMap123, true );

        // Then
        assertThat( newMap123.size(), is( 3 ) );
        assertThat( newMap123.get( 1 ), is( 2 ) );
        assertThat( newMap123.get( 2 ), is( 2 ) );
        assertThat( newMap123.get( 3 ), is( 3 ) );
    }

    @Test
    public void shouldMergeAllMapToMapWithoutOverwrite()
    {
        // Given
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        // When
        Map<Integer, Integer> newMap123 = new HashMap<Integer, Integer>();
        newMap123.put( 1, 2 );
        newMap123 = MapUtils.mergeMaps( map123, newMap123, false );

        // Then
        assertThat( newMap123.size(), is( 3 ) );
        assertThat( newMap123.get( 1 ), is( 1 ) );
        assertThat( newMap123.get( 2 ), is( 2 ) );
        assertThat( newMap123.get( 3 ), is( 3 ) );
    }

    @Test
    public void shouldMergeAllMapToMapAndSumValuesByOne()
    {
        // Given
        Map<Integer, Integer> map123 = new HashMap<Integer, Integer>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        Function2<Integer, Integer, Integer> sumFun = new Function2<Integer, Integer, Integer>()
        {
            @Override
            public Integer apply( Integer from1, Integer from2 )
            {
                return from1 + from2;
            }
        };

        // When
        Map<Integer, Integer> newMap123 = new HashMap<Integer, Integer>();
        newMap123.put( 1, 9 );
        newMap123 = MapUtils.mergeMaps( map123, newMap123, sumFun );

        // Then
        assertThat( newMap123.size(), is( 3 ) );
        assertThat( newMap123.get( 1 ), is( 10 ) );
        assertThat( newMap123.get( 2 ), is( 2 ) );
        assertThat( newMap123.get( 3 ), is( 3 ) );
    }
}
