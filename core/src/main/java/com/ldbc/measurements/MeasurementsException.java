package com.ldbc.measurements;

public class MeasurementsException extends Exception
{
    private static final long serialVersionUID = 6646883591588721475L;

    public MeasurementsException( String message )
    {
        super( message );
    }

    public MeasurementsException()
    {
        super();
    }

    public MeasurementsException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public MeasurementsException( Throwable cause )
    {
        super( cause );
    }
}
