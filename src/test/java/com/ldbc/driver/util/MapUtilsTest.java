package com.ldbc.driver.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class MapUtilsTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldBeEqualAfterMapToPropertiesThenPropertiesToMap()
    {
        // Given
        Map<Integer,Integer> map123 = new HashMap<>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        // When
        Properties properties123 = MapUtils.mapToProperties( map123 );
        Map<Integer,Integer> afterMap123 = MapUtils.propertiesToMap( properties123 );

        // Then
        assertThat( map123.size(), equalTo( afterMap123.size() ) );
        assertThat( afterMap123.containsKey( 1 ), is( true ) );
        assertThat( afterMap123.containsKey( 2 ), is( true ) );
        assertThat( afterMap123.containsKey( 3 ), is( true ) );
        assertThat( afterMap123.get( 1 ), equalTo( map123.get( 1 ) ) );
        assertThat( afterMap123.get( 2 ), equalTo( map123.get( 2 ) ) );
        assertThat( afterMap123.get( 3 ), equalTo( map123.get( 3 ) ) );
    }

    @Test
    public void shouldNotCopyExcludedKeys()
    {
        // Given
        Map<Integer,Integer> map123 = new HashMap<>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        Set<Integer> excludeKey2 = new HashSet<Integer>( Arrays.asList( new Integer[]{2} ) );

        // When
        Map<Integer,Integer> map13 = MapUtils.copyExcludingKeys( map123, excludeKey2 );

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
        Map<Integer,Integer> map123 = new HashMap<>();
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
        Map<Integer,Integer> map123 = new HashMap<>();
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
        Map<Integer,Integer> map123 = new HashMap<>();
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
        Map<Integer,Integer> map123 = new HashMap<>();
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
        Map<Integer,Integer> map123 = new HashMap<>();
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
        Map<Integer,Integer> map123 = new HashMap<Integer,Integer>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        // When
        Map<Integer,Integer> newMap123 = new HashMap<>();
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
        Map<Integer,Integer> map123 = new HashMap<>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        // When
        Map<Integer,Integer> newMap123 = new HashMap<>();
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
        Map<Integer,Integer> map123 = new HashMap<>();
        map123.put( 1, 1 );
        map123.put( 2, 2 );
        map123.put( 3, 3 );

        Function2<Integer,Integer,Integer,RuntimeException> sumFun =
                new Function2<Integer,Integer,Integer,RuntimeException>()
                {
                    @Override
                    public Integer apply( Integer from1, Integer from2 )
                    {
                        return from1 + from2;
                    }
                };

        // When
        Map<Integer,Integer> newMap123 = new HashMap<>();
        newMap123.put( 1, 9 );
        newMap123 = MapUtils.mergeMaps( map123, newMap123, sumFun );

        // Then
        assertThat( newMap123.size(), is( 3 ) );
        assertThat( newMap123.get( 1 ), is( 10 ) );
        assertThat( newMap123.get( 2 ), is( 2 ) );
        assertThat( newMap123.get( 3 ), is( 3 ) );
    }

    @Test
    public void loadPropertiesFileToMapShouldWork() throws IOException
    {
        // Given
        Properties properties = new Properties();
        properties.put( "one", "1" );
        properties.put( "two", "2" );
        File propertiesFile = temporaryFolder.newFile();
        properties.store( new FileOutputStream( propertiesFile ), "no comment" );

        // When
        Map<String,String> map = MapUtils.loadPropertiesToMap( propertiesFile );

        // Then
        assertThat( map.get( "one" ), equalTo( "1" ) );
        assertThat( map.get( "two" ), equalTo( "2" ) );
    }

    @Test
    public void loadPropertiesStringToMapShouldWork() throws IOException
    {
        // Given
        String propertiesString = "one=1\ntwo=2";

        // When
        Map<String,String> map = MapUtils.loadPropertiesStringToMap( propertiesString );

        // Then
        assertThat( map.get( "one" ), equalTo( "1" ) );
        assertThat( map.get( "two" ), equalTo( "2" ) );
    }

    @Test
    public void loadPropertiesInputStreamToMapShouldWork() throws IOException
    {
        // Given
        String propertiesString = "one=1\ntwo=2";
        InputStream propertiesInputStream =
                new ByteArrayInputStream( propertiesString.getBytes( StandardCharsets.UTF_8 ) );

        // When
        Map<String,String> map = MapUtils.loadPropertiesInputStreamToMap( propertiesInputStream );

        // Then
        assertThat( map.get( "one" ), equalTo( "1" ) );
        assertThat( map.get( "two" ), equalTo( "2" ) );
    }

    @Test
    public void changeTypesShouldWorkWithToString() throws IOException
    {
        // Given
        Map<Integer,Long> longMap = new HashMap<>();
        longMap.put( 1, 10l );
        longMap.put( 2, 20l );
        Map<Integer,String> stringMapRight = new HashMap<>();
        stringMapRight.put( 1, "10" );
        stringMapRight.put( 2, "20" );
        Map<Integer,String> stringMapWrong = new HashMap<>();
        stringMapWrong.put( 1, "10" );
        stringMapWrong.put( 2, "21" );

        // When
        Map<Integer,String> stringMapComputed = MapUtils.UNSAFE_changeTypes(
                longMap,
                TypeChangeFun.IDENTITY,
                TypeChangeFun.TO_STRING
        );

        // Then
        assertThat( stringMapRight, equalTo( stringMapComputed ) );
        assertThat( stringMapRight, not( equalTo( stringMapWrong ) ) );
    }

    @Test
    public void changeTypesShouldWorkWithMapping() throws IOException
    {
        // Given
        Map<Integer,Long> longMap = new HashMap<>();
        longMap.put( 1, 10l );
        longMap.put( 2, 20l );
        Map<Integer,String> mapping = new HashMap<>();
        mapping.put( 1, "foo" );
        mapping.put( 2, "bar" );
        Map<String,String> expectedMap = new HashMap<>();
        expectedMap.put( "foo", "10" );
        expectedMap.put( "bar", "20" );

        // When
        Map<String,String> computedMap = MapUtils.UNSAFE_changeTypes(
                longMap,
                TypeChangeFun.mapped( mapping ),
                TypeChangeFun.TO_STRING
        );

        // Then
        assertThat( computedMap, equalTo( expectedMap ) );
    }

    @Test
    public void shouldSwitchKeysAndValues() throws IOException
    {
        // Given
        Map<Integer,Long> originalMap = new HashMap<>();
        originalMap.put( 1, 10l );
        originalMap.put( 2, 20l );
        Map<Long,Integer> expectedMap = new HashMap<>();
        expectedMap.put( 10l, 1 );
        expectedMap.put( 20l, 2 );

        // When
        Map<Long,Integer> computedMap = MapUtils.UNSAFE_switchKeysAndValues( originalMap );

        // Then
        assertThat( computedMap, equalTo( expectedMap ) );
    }
}
