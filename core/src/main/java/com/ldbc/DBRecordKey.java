package com.ldbc;

import com.ldbc.util.HashUtils;

// TODO test
public class DBRecordKey
{
    private final String keyNumber;
    private final String hashedKeyNumber;

    public DBRecordKey( int keyNumber )
    {
        this.keyNumber = Long.toString( keyNumber );
        this.hashedKeyNumber = Long.toString( HashUtils.FNVhash32( keyNumber ) );
    }

    public DBRecordKey( long keyNumber )
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
