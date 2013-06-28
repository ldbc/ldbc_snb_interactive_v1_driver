package com.ldbc.driver.util;

public class Duration implements MultipleTimeUnitProvider
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
        return Duration.fromNano( secondTime.asNano() - firstTime.asNano() );
    }

    private final long nanoDuration;

    private Duration( long ns )
    {
        this.nanoDuration = ns;
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
        return TimeUnitConvertor.nanoToSecond( nanoDuration );
    }

    @Override
    public long asMilli()
    {
        return TimeUnitConvertor.nanoToMilli( nanoDuration );
    }

    @Override
    public long asMicro()
    {
        return TimeUnitConvertor.nanoToMicro( nanoDuration );
    }

    @Override
    public long asNano()
    {
        return nanoDuration;
    }

    @Override
    public String toString()
    {
        return nanoDuration + "(ns)";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( nanoDuration ^ ( nanoDuration >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Duration other = (Duration) obj;
        if ( nanoDuration != other.nanoDuration ) return false;
        return true;
    }
}
