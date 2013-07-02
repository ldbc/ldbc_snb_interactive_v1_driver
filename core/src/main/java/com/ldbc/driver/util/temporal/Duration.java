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

    public static Duration durationBetween( Time firstTime, Time secondTime )
    {
        if ( secondTime.asNano() < firstTime.asNano() )
        {
            // TODO different Exception type
            throw new RuntimeException( "Second Time must be later than First Time" );
        }
        return Duration.fromNano( secondTime.asNano() - firstTime.asNano() );
    }

    private final Long durationNano;

    private Duration( long ns )
    {
        this.durationNano = ns;
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
        return TimeUnitConvertor.nanoToSecond( durationNano );
    }

    @Override
    public long asMilli()
    {
        return TimeUnitConvertor.nanoToMilli( durationNano );
    }

    @Override
    public long asMicro()
    {
        return TimeUnitConvertor.nanoToMicro( durationNano );
    }

    @Override
    public long asNano()
    {
        return durationNano;
    }

    @Override
    public String toString()
    {
        return durationNano + "(ns)";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( durationNano ^ ( durationNano >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Duration other = (Duration) obj;
        if ( false == durationNano.equals( other.durationNano ) ) return false;
        return true;
    }

    @Override
    public int compareTo( Duration o )
    {
        return durationNano.compareTo( o.durationNano );
    }
}
