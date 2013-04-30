package com.yahoo.ycsb;

import com.yahoo.ycsb.util.Utils;

// TODO test
public class DBRecordKey
{
    private final String keyNumber;
    private final String hashedKeyNumber;

    public DBRecordKey( long keyNumber )
    {
        this.keyNumber = Long.toString( keyNumber );
        this.hashedKeyNumber = Long.toString( Utils.hash( keyNumber ) );
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
