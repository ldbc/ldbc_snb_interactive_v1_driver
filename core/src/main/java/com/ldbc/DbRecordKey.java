package com.ldbc;

import com.ldbc.util.HashUtils;

// TODO test
// TODO move hash() to something like a HashGeneratorWrapper
public class DbRecordKey
{
    private final String keyNumber;
    private final String hashedKeyNumber;

    public DbRecordKey( int keyNumber )
    {
        this.keyNumber = Long.toString( keyNumber );
        this.hashedKeyNumber = Long.toString( HashUtils.FNVhash32( keyNumber ) );
    }

    public DbRecordKey( long keyNumber )
    {
        this.keyNumber = Long.toString( keyNumber );
        this.hashedKeyNumber = Long.toString( HashUtils.FNVhash64( keyNumber ) );
    }

    public String get()
    {
        return keyNumber;
    }

    public String getPrefixed( String prefix )
    {
        return prefix + keyNumber;
    }

    public String getHashed()
    {
        return hashedKeyNumber;
    }

    public String getHashedPrefixed( String prefix )
    {
        return prefix + hashedKeyNumber;
    }
}
