package com.ldbc.driver.util.temporal;

import java.util.concurrent.TimeUnit;

public class Time implements Comparable<Time>, MultipleTimeUnitProvider<Time>
{
    public static Time now()
    {
        return Time.fromNano( Temporal.convert( System.currentTimeMillis(), TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS ) );
    }

    public static Time fromNano( long ns )
    {
        return new Time( ns );
    }

    public static Time fromMicro( long us )
    {
        return Time.fromNano( Temporal.convert( us, TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS ) );
    }

    public static Time fromMilli( long ms )
    {
        return Time.fromNano( Temporal.convert( ms, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS ) );
    }

    public static Time fromSeconds( long s )
    {
        return Time.fromNano( Temporal.convert( s, TimeUnit.SECONDS, TimeUnit.NANOSECONDS ) );
    }

    public static Time fromMinutes( long m )
    {
        return Time.fromNano( Temporal.convert( m, TimeUnit.MINUTES, TimeUnit.NANOSECONDS ) );
    }

    public static Time from( TimeUnit timeUnit, long unitOfTime )
    {
        return Time.fromNano( Temporal.convert( unitOfTime, timeUnit, TimeUnit.NANOSECONDS ) );
    }

    private final Temporal time;

    private Time( long timeNano )
    {
        this.time = Temporal.fromNano( timeNano );
    }

    @Override
    public String toString()
    {
        return time.asNano() + "(ns)";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( time.asNano() ^ ( time.asNano() >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Time other = (Time) obj;
        if ( false == time.equals( other.time ) ) return false;
        return true;
    }

    @Override
    public int compareTo( Time o )
    {
        return new Long( time.asNano() ).compareTo( o.time.asNano() );
    }

    @Override
    public long asNano()
    {
        return time.asNano();
    }

    @Override
    public long asMicro()
    {
        return time.asMicro();
    }

    @Override
    public long asMilli()
    {
        return time.asMilli();
    }

    @Override
    public long asSeconds()
    {
        return time.asSeconds();
    }

    @Override
    public long as( TimeUnit timeUnit )
    {
        return time.as( timeUnit );
    }

    @Override
    public boolean greatThan( Time other )
    {
        return time.greatThan( other.time );
    }

    @Override
    public boolean lessThan( Time other )
    {
        return time.lessThan( other.time );
    }

    @Override
    public Duration greaterBy( Time other )
    {
        return time.greaterBy( other.time );
    }

    @Override
    public Duration lessBy( Time other )
    {
        return time.lessBy( other.time );
    }

    @Override
    public Time plus( Duration duration )
    {
        return Time.fromNano( time.plus( duration ).asNano() );
    }

    @Override
    public Time minus( Duration duration )
    {
        return Time.fromNano( time.minus( duration ).asNano() );
    }
}
