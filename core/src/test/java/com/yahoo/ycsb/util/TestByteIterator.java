package com.yahoo.ycsb.util;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Test;

import com.yahoo.ycsb.util.ByteIterator;
import com.yahoo.ycsb.util.RandomByteIterator;


import static org.junit.Assert.assertEquals;

public class TestByteIterator
{
    @Test
    public void testRandomByteIterator()
    {
        RandomDataGenerator random = new RandomDataGenerator();

        int size = 100;
        ByteIterator itor = new RandomByteIterator( size, random );
        assertEquals( true, itor.hasNext() );
        assertEquals( size, itor.bytesLeft() );
        assertEquals( size, itor.toString().getBytes().length );
        assertEquals( false, itor.hasNext() );
        assertEquals( 0, itor.bytesLeft() );

        itor = new RandomByteIterator( size, random );
        assertEquals( size, itor.toArray().length );
        assertEquals( false, itor.hasNext() );
        assertEquals( 0, itor.bytesLeft() );
    }
}
