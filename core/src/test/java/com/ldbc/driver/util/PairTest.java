package com.ldbc.driver.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ldbc.driver.util.Pair;

public class PairTest
{
    @Test
    public void equalsTest()
    {
        // Given
        Pair<String, Integer> p1a = Pair.create( "1", 1 );
        Pair<String, Integer> p2 = Pair.create( "2", 2 );
        Pair<String, Integer> p3 = Pair.create( "1", 2 );
        Pair<String, Integer> p4 = Pair.create( "2", 1 );
        Pair<String, Integer> p1b = Pair.create( "1", 1 );

        // When

        // Then
        assertEquals( true, p1a.equals( p1a ) );
        assertEquals( false, p1a.equals( p2 ) );
        assertEquals( false, p1a.equals( p3 ) );
        assertEquals( false, p1a.equals( p4 ) );
        assertEquals( true, p1a.equals( p1b ) );
    }

    @Test
    public void getterTest()
    {
        // Given
        Pair<String, Integer> p = Pair.create( "1", 1 );

        // When

        // Then
        assertEquals( "1", p._1() );
        assertEquals( new Integer( 1 ), p._2() );
    }
}
