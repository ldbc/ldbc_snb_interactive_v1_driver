package com.ldbc.driver.data;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Test;

import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.data.RandomByteIterator;

import static org.junit.Assert.assertEquals;

public class TestByteIterator
{
    @Test
    public void testRandomByteIterator()
    {
        RandomDataGenerator random = new RandomDataGenerator();

        int size = 100;
        ByteIterator byteIterator = new RandomByteIterator( size, random );
        assertEquals( true, byteIterator.hasNext() );
        assertEquals( size, byteIterator.bytesRemaining() );
        assertEquals( size, byteIterator.toString().getBytes().length );
        assertEquals( false, byteIterator.hasNext() );
        assertEquals( 0, byteIterator.bytesRemaining() );

        byteIterator = new RandomByteIterator( size, random );
        assertEquals( size, byteIterator.toArray().length );
        assertEquals( false, byteIterator.hasNext() );
        assertEquals( 0, byteIterator.bytesRemaining() );
    }
}
