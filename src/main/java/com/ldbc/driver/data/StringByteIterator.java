package com.ldbc.driver.data;

public class StringByteIterator extends ByteIterator
{
    final String string;
    int offset;

    public StringByteIterator( String string )
    {
        this.string = string;
        this.offset = 0;
    }

    @Override
    public boolean hasNext()
    {
        return offset < string.length();
    }

    @Override
    public byte nextByte()
    {
        return (byte) string.charAt( offset++ );
    }

    @Override
    public long bytesRemaining()
    {
        return string.length() - offset;
    }

    /**
     * Specialization of general purpose toString() to avoid unnecessary copies.
     * <p>
     * Creating a new StringByteIterator, then calling toString() yields the
     * original String object, and does not perform any copies or String
     * conversion operations.
     * </p>
     */
    @Override
    public String toString()
    {
        if ( offset > 0 )
        {
            return super.toString();
        }
        else
        {
            return string;
        }
    }
}
