package com.yahoo.ycsb;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestByteIterator
{
    @Test
    public void testRandomByteIterator()
    {
        int size = 100;
        ByteIterator itor = new RandomByteIterator( size );
        assertEquals( true, itor.hasNext() );
        assertEquals( size, itor.bytesLeft() );
        assertEquals( size, itor.toString().getBytes().length );
        assertEquals( false, itor.hasNext() );
        assertEquals( 0, itor.bytesLeft() );

        itor = new RandomByteIterator( size );
        assertEquals( size, itor.toArray().length );
        assertEquals( false, itor.hasNext() );
        assertEquals( 0, itor.bytesLeft() );
    }
}
