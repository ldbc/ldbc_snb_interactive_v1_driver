package com.ldbc.driver.data;

import java.io.InputStream;

public class InputStreamByteIterator extends ByteIterator
{
    final long length;
    final InputStream inputStream;
    long offset;

    public InputStreamByteIterator( InputStream inputStream, long length )
    {
        this.length = length;
        this.inputStream = inputStream;
        offset = 0;
    }

    @Override
    public boolean hasNext()
    {
        return offset < length;
    }

    @Override
    public byte nextByte()
    {
        int nextByte;
        try
        {
            nextByte = inputStream.read();
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( e );
        }
        if ( nextByte == -1 )
        {
            throw new IllegalStateException( "Past EOF!" );
        }
        offset++;
        return (byte) nextByte;
    }

    @Override
    public long bytesRemaining()
    {
        return length - offset;
    }

}
