package com.ldbc.driver.measurements;

public class MetricException extends RuntimeException
{
    private static final long serialVersionUID = -5243628117143814942L;

    public MetricException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public MetricException( String message )
    {
        super( message );
    }

    public MetricException( Throwable cause )
    {
        super( cause );
    }

    public MetricException()
    {
        super();
    }

}
