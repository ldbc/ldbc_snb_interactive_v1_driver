package com.yahoo.ycsb;

// TODO test
public class DBKey
{
    private final String keyNumber;
    private final String hashedKeyNumber;

    public DBKey( long keyNumber )
    {
        this.keyNumber = Long.toString( keyNumber );
        this.hashedKeyNumber = Long.toString( HashUtils.hash( keyNumber ) );
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
