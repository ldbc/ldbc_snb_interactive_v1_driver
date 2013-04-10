package com.yahoo.ycsb;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Test;
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
