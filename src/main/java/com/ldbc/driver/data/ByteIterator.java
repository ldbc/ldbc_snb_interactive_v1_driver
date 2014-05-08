package com.ldbc.driver.data;

import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.GeneratorException;

/**
 * YCSB-specific buffer class. ByteIterators are designed to support efficient
 * field generation, and to allow backend drivers that can stream fields
 * (instead of materializing them in RAM) to do so.
 * <p>
 * YCSB originaFlly used String objects to represent field values. This led to
 * two performance issues.
 * </p>
 * <p>
 * First, it leads to unnecessary conversions between UTF-16 and UTF-8, both
 * during field generation, and when passing data to byte-based backend drivers.
 * </p>
 * <p>
 * Second, Java strings are represented internally using UTF-16, and are built
 * by appending to a growable array type (StringBuilder or StringBuffer), then
 * calling a toString() method. This leads to a 4x memory overhead as field
 * values are being built, which prevented YCSB from driving large object
 * stores.
 * </p>
 * The StringByteIterator class contains a number of convenience methods for
 * backend drivers that convert between Map&lt;String,String&gt; and
 * Map&lt;String,ByteBuffer&gt;.
 * 
 * @author sears
 */
public abstract class ByteIterator
{
    public abstract boolean hasNext();

    public abstract long bytesRemaining();

    public abstract byte nextByte();

    // return byte offset immediately after the last valid byte
    public int nextBuffer( byte[] buffer, int bufferOffset )
    {
        int bufferIndex = bufferOffset;
        while ( bufferIndex < buffer.length && hasNext() )
        {
            buffer[bufferIndex] = nextByte();
            bufferIndex++;
        }
        return bufferIndex;
    }

    // Consumes remaining contents of this object and returns them as string
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        while ( this.hasNext() )
        {
            sb.append( (char) nextByte() );
        }
        return sb.toString();
    }

    // Consumes remaining contents of this object and returns them as byte array
    public byte[] toArray()
    {
        try
        {
            long remaining = (int) bytesRemaining();
            if ( remaining >= Integer.MAX_VALUE )
            {
                throw new WorkloadException( "Bytes remaining exceed maximum possible array length" );
            }
            byte[] remainingBytes = new byte[(int) remaining];
            int offset = 0;
            while ( offset < remainingBytes.length )
            {
                offset = nextBuffer( remainingBytes, offset );
            }
            return remainingBytes;
        }
        catch ( Exception e )
        {
            throw new GeneratorException( "Error encountered while copying remaining bytes to array", e );
        }
    }
}
