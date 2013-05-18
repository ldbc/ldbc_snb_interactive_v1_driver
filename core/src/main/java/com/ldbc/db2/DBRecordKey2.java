package com.ldbc.db2;

import com.ldbc.util.HashUtils;

// TODO test
public class DBRecordKey2
{
    private final String keyNumber;
    private final String hashedKeyNumber;

    public DBRecordKey2( int keyNumber )
    {
        this.keyNumber = Long.toString( keyNumber );
        this.hashedKeyNumber = Long.toString( HashUtils.FNVhash32( keyNumber ) );
    }

    public DBRecordKey2( long keyNumber )
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
