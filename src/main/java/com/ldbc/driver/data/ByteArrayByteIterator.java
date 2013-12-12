package com.ldbc.driver.data;

import com.ldbc.driver.generator.GeneratorException;

public class ByteArrayByteIterator extends ByteIterator
{
    final int maxByteIndex;
    final byte[] bytes;
    int offset;

    public ByteArrayByteIterator( byte[] bytes )
    {
        this( bytes, 0, bytes.length );
    }

    public ByteArrayByteIterator( byte[] bytes, int offset, int length )
    {
        this.bytes = bytes;
        this.offset = offset;
        this.maxByteIndex = offset + length;
        if ( maxByteIndex >= bytes.length )
        {
            throw new GeneratorException( "offset + length exceeds bytes.length" );
        }
    }

    @Override
    public boolean hasNext()
    {
        return offset < maxByteIndex;
    }

    @Override
    public byte nextByte()
    {
        return bytes[offset++];
    }

    @Override
    public long bytesRemaining()
    {
        return maxByteIndex - offset;
    }

}
