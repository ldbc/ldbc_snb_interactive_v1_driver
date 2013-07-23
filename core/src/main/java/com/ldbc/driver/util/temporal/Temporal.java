package com.ldbc.driver.util.temporal;

public class Temporal implements MultipleTimeUnitProvider<Temporal>
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

    @Override
    public long asNano()
    {
        return nanoValue;
    }

    @Override
    public long asMicro()
    {
        return TimeUnitConvertor.nanoToMicro( nanoValue );
    }

    @Override
    public long asMilli()
    {
        return TimeUnitConvertor.nanoToMilli( nanoValue );
    }

    @Override
    public long asSeconds()
    {
        return TimeUnitConvertor.nanoToSecond( nanoValue );
    }

    @Override
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
    public boolean greatThan( Temporal other )
    {
        return this.asNano() > other.asNano();
    }

    @Override
    public boolean lessThan( Temporal other )
    {
        return this.asNano() < other.asNano();
    }

    @Override
    public Duration greaterBy( Temporal other )
    {
        return Duration.fromNano( this.asNano() - other.asNano() );
    }

    @Override
    public Duration lessBy( Temporal other )
    {
        return Duration.fromNano( other.asNano() - this.asNano() );
    }

    @Override
    public Temporal plus( Duration duration )
    {
        return fromNano( this.asNano() + duration.asNano() );
    }

    @Override
    public Temporal minus( Duration duration )
    {
        return fromNano( this.asNano() - duration.asNano() );
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
