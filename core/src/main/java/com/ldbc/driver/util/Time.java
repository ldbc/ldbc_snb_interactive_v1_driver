package com.ldbc.driver.util;

public class Time
{

    private final long nanoDuration;

    public static Time fromNano( long ns )
    {
        return new Time( ns );
    }

    public static Time fromMicro( long us )
    {
        return new Time( us * 1000 );
    }

    public static Time fromMilli( long ms )
    {
        return new Time( ms * 1000000 );
    }

    public static Time fromSeconds( long s )
    {
        return new Time( s * 1000000000 );
    }

    private Time( long ns )
    {
        this.nanoDuration = ns;
    }

    public Time minus( Time that )
    {
        return Time.fromNano( this.toNano() - that.toNano() );
    }

    public Time plus( Time that )
    {
        return Time.fromNano( this.toNano() + that.toNano() );
    }

    public long toNano()
    {
        return nanoDuration;
    }

    public long toMicro()
    {
        return toNano() / 1000;
    }

    public long toMilli()
    {
        return toMicro() / 1000;
    }

    public long toSeconds()
    {
        return toMilli() / 1000;
    }
}
