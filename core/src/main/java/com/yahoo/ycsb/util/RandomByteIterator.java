/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *                                                              
 * http://www.apache.org/licenses/LICENSE-2.0
 *                                                            
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.util;

import org.apache.commons.math3.random.RandomDataGenerator;


/**
 * A ByteIterator that generates a random sequence of bytes.
 */
public class RandomByteIterator extends ByteIterator
{
    private long length;
    private long offset;
    private int bufferOffset;
    private byte[] buffer;

    private final RandomDataGenerator rdg;

    public RandomByteIterator( long length, RandomDataGenerator rdg )
    {
        this.length = length;
        this.buffer = new byte[6];
        this.bufferOffset = buffer.length;
        this.rdg = rdg;
        fillBytes();
        this.offset = 0;
    }

    @Override
    public boolean hasNext()
    {
        return ( offset + bufferOffset ) < length;
    }

    private void fillBytesImpl( byte[] buffer, int base )
    {
        // TODO remove
        // int bytes = Utils.random().nextInt();
        int bytes = rdg.nextInt( 0, 2 ^ 32 );
        try
        {
            buffer[base + 0] = (byte) ( ( ( bytes ) & 31 ) + ' ' );
            buffer[base + 1] = (byte) ( ( ( bytes >> 5 ) & 31 ) + ' ' );
            buffer[base + 2] = (byte) ( ( ( bytes >> 10 ) & 31 ) + ' ' );
            buffer[base + 3] = (byte) ( ( ( bytes >> 15 ) & 31 ) + ' ' );
            buffer[base + 4] = (byte) ( ( ( bytes >> 20 ) & 31 ) + ' ' );
            buffer[base + 5] = (byte) ( ( ( bytes >> 25 ) & 31 ) + ' ' );
        }
        catch ( ArrayIndexOutOfBoundsException e )
        { /* ignore it */
            // TODO wtf is going on here?!
        }
    }

    private void fillBytes()
    {
        if ( bufferOffset == buffer.length )
        {
            fillBytesImpl( buffer, 0 );
            bufferOffset = 0;
            offset += buffer.length;
        }
    }

    public byte nextByte()
    {
        fillBytes();
        bufferOffset++;
        return buffer[bufferOffset - 1];
    }

    @Override
    public int nextBuf( byte[] buffer, int bufferOffset )
    {
        int ret;
        if ( length - offset < buffer.length - bufferOffset )
        {
            ret = (int) ( length - offset );
        }
        else
        {
            ret = buffer.length - bufferOffset;
        }
        int i;
        for ( i = 0; i < ret; i += 6 )
        {
            fillBytesImpl( buffer, i + bufferOffset );
        }
        offset += ret;
        return ret + bufferOffset;
    }

    @Override
    public long bytesLeft()
    {
        return length - offset - bufferOffset;
    }
}
