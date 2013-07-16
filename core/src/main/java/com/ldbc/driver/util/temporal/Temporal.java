package com.ldbc.driver.util.temporal;

public class Temporal implements MultipleTimeUnitProvider
{
    public static Temporal fromNano( long ns )
    {
        return new Temporal( ns );
    }

    private final long nanoValue;

    private Temporal( long nanoValue )
    {
        this.nanoValue = nanoValue;
    }

    public long asNano()
    {
        return nanoValue;
    }

    public long asMicro()
    {
        return TimeUnitConvertor.nanoToMicro( nanoValue );
    }

    public long asMilli()
    {
        return TimeUnitConvertor.nanoToMilli( nanoValue );
    }

    public long asSeconds()
    {
        return TimeUnitConvertor.nanoToSecond( nanoValue );
    }

    public long as( TimeUnit timeUnit )
    {
        switch ( timeUnit )
        {
        case NANO:
            return this.asNano();
        case MICRO:
            return this.asMicro();
        case MILLI:
            return this.asMilli();
        case SECOND:
            return this.asSeconds();
        }
        throw new RuntimeException( "Unexpected error - unsupported TimeUnit" );
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( nanoValue ^ ( nanoValue >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Temporal other = (Temporal) obj;
        if ( nanoValue != other.nanoValue ) return false;
        return true;
    }
}
