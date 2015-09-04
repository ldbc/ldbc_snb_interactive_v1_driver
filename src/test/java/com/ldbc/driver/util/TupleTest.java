package com.ldbc.driver.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TupleTest
{
    @Test
    public void tuple2EqualityTest()
    {
        // Given
        Tuple2<String, Integer> p1a = Tuple.tuple2( "1", 1 );
        Tuple2<String, Integer> p2 = Tuple.tuple2( "2", 2 );
        Tuple2<String, Integer> p3 = Tuple.tuple2( "1", 2 );
        Tuple2<String, Integer> p4 = Tuple.tuple2( "2", 1 );
        Tuple2<String, Integer> p1b = Tuple.tuple2( "1", 1 );

        // When

        // Then
        assertEquals( true, p1a.equals( p1a ) );
        assertEquals( false, p1a.equals( p2 ) );
        assertEquals( false, p1a.equals( p3 ) );
        assertEquals( false, p1a.equals( p4 ) );
        assertEquals( true, p1a.equals( p1b ) );
    }

    @Test
    public void tuple2GetterTest()
    {
        // Given
        Tuple2<String, Integer> p = Tuple.tuple2( "1", 1 );

        // When

        // Then
        assertEquals( "1", p._1() );
        assertEquals( new Integer( 1 ), p._2() );
    }

    @Test
    public void tuple3EqualityTest()
    {
        // Given
        Tuple3<String, Integer, Boolean> t1falsea = Tuple.tuple3( "1", 1, false );
        Tuple3<String, Integer, Boolean> t1true = Tuple.tuple3( "1", 1, true );
        Tuple3<String, Integer, Boolean> t2 = Tuple.tuple3( "2", 2, false );
        Tuple3<String, Integer, Boolean> t3 = Tuple.tuple3( "1", 2, false );
        Tuple3<String, Integer, Boolean> t4 = Tuple.tuple3( "2", 1, false );
        Tuple3<String, Integer, Boolean> t1falseb = Tuple.tuple3( "1", 1, false );

        // When

        // Then
        assertEquals( true, t1falsea.equals( t1falsea ) );
        assertEquals( false, t1falsea.equals( t1true ) );
        assertEquals( false, t1falsea.equals( t2 ) );
        assertEquals( false, t1falsea.equals( t3 ) );
        assertEquals( false, t1falsea.equals( t4 ) );
        assertEquals( true, t1falsea.equals( t1falseb ) );
    }

    @Test
    public void tuple3GetterTest()
    {
        // Given
        Tuple3<String, Integer, Boolean> t = Tuple.tuple3( "1", 1, true );

        // When

        // Then
        assertEquals( "1", t._1() );
        assertEquals( new Integer( 1 ), t._2() );
        assertEquals( true, t._3() );
    }
}
