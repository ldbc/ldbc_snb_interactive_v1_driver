package com.ldbc.driver.util.temporal;

import java.util.concurrent.TimeUnit;

public class Duration implements Comparable<Duration>, MultipleTimeUnitProvider<Duration>
{
    public static Duration fromNano( long ns )
    {
        return new Duration( ns );
    }

    public static Duration fromMicro( long us )
    {
        return Duration.fromNano( Temporal.convert( us, TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS ) );
    }

    public static Duration fromMilli( long ms )
    {
        return Duration.fromNano( Temporal.convert( ms, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS ) );
    }

    public static Duration fromSeconds( long s )
    {
        return Duration.fromNano( Temporal.convert( s, TimeUnit.SECONDS, TimeUnit.NANOSECONDS ) );
    }

    public static Duration fromMinutes( long m )
    {
        return Duration.fromNano( Temporal.convert( m, TimeUnit.MINUTES, TimeUnit.NANOSECONDS ) );
    }

    public static Duration from( TimeUnit timeUnit, long unitOfTime )
    {
        return Duration.fromNano( Temporal.convert( unitOfTime, timeUnit, TimeUnit.NANOSECONDS ) );
    }

    private final Temporal duration;

    private Duration( long ns )
    {
        this.duration = Temporal.fromNano( ns );
    }

    @Override
    public long asSeconds()
    {
        return duration.asSeconds();
    }

    @Override
    public long asMilli()
    {
        return duration.asMilli();
    }

    @Override
    public long asMicro()
    {
        return duration.asMicro();
    }

    @Override
    public long asNano()
    {
        return duration.asNano();
    }

    @Override
    public String toString()
    {
        long m = TimeUnit.MILLISECONDS.toMinutes( duration.asMilli() );
        long s = TimeUnit.MILLISECONDS.toSeconds( duration.asMilli() ) - TimeUnit.MINUTES.toSeconds( m );
        long ms = duration.asMilli() - TimeUnit.MINUTES.toMillis( m ) - TimeUnit.SECONDS.toMillis( s );
        return String.format( "%02d:%02d.%03d (m:s.ms)", m, s, ms );
    }

    @Override
    public long as( TimeUnit timeUnit )
    {
        return duration.as( timeUnit );
    }

    @Override
    public boolean greatThan( Duration other )
    {
        return duration.greatThan( other.duration );
    }

    @Override
    public boolean lessThan( Duration other )
    {
        return duration.lessThan( other.duration );
    }

    @Override
    public Duration greaterBy( Duration other )
    {
        return this.duration.greaterBy( other.duration );
    }

    @Override
    public Duration lessBy( Duration other )
    {
        return this.duration.lessBy( other.duration );
    }

    @Override
    public Duration plus( Duration duration )
    {
        return Duration.fromNano( this.duration.plus( duration ).asNano() );
    }

    @Override
    public Duration minus( Duration duration )
    {
        return Duration.fromNano( this.duration.minus( duration ).asNano() );
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( duration.asNano() ^ ( duration.asNano() >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Duration other = (Duration) obj;
        if ( false == this.duration.equals( other.duration ) ) return false;
        return true;
    }

    @Override
    public int compareTo( Duration other )
    {
        return new Long( this.duration.asNano() ).compareTo( other.duration.asNano() );
    }
}
