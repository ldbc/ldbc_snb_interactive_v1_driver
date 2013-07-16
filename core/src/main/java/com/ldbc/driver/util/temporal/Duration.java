package com.ldbc.driver.util.temporal;

public class Duration implements Comparable<Duration>, MultipleTimeUnitProvider
{
    public static Duration fromNano( long ns )
    {
        return new Duration( ns );
    }

    public static Duration fromMicro( long us )
    {
        return new Duration( TimeUnitConvertor.nanoFromMicro( us ) );
    }

    public static Duration fromMilli( long ms )
    {
        return new Duration( TimeUnitConvertor.nanoFromMilli( ms ) );
    }

    public static Duration fromSeconds( long s )
    {
        return new Duration( TimeUnitConvertor.nanoFromSecond( s ) );
    }

    public static Duration from( TimeUnit timeUnit, long unitOfTime )
    {
        switch ( timeUnit )
        {
        case NANO:
            return Duration.fromNano( unitOfTime );
        case MICRO:
            return Duration.fromMicro( unitOfTime );
        case MILLI:
            return Duration.fromMilli( unitOfTime );
        case SECOND:
            return Duration.fromSeconds( unitOfTime );
        }
        throw new RuntimeException( "Unexpected error - unsupported TimeUnit" );
    }

    public static Duration durationBetween( Time firstTime, Time secondTime )
    {
        if ( secondTime.asNano() < firstTime.asNano() )
        {
            // TODO different Exception type
            throw new RuntimeException( "Second Time must be later than First Time" );
        }
        return Duration.fromNano( secondTime.asNano() - firstTime.asNano() );
    }

    private final Temporal duration;

    private Duration( long ns )
    {
        this.duration = Temporal.fromNano( ns );
    }

    public Duration minus( Duration that )
    {
        return Duration.fromNano( this.asNano() - that.asNano() );
    }

    public Duration plus( Duration that )
    {
        return Duration.fromNano( this.asNano() + that.asNano() );
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
        return duration.asNano() + "(ns)";
    }

    @Override
    public long as( TimeUnit timeUnit )
    {
        return duration.as( timeUnit );
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
    public int compareTo( Duration o )
    {
        return new Long( duration.asNano() ).compareTo( o.duration.asNano() );
    }
}
