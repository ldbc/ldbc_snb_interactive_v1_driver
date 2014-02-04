package com.ldbc.driver.temporal;

public class TemporalException extends RuntimeException
{
    private static final long serialVersionUID = 8844396756042772132L;

    public TemporalException( String message )
    {
        super( message );
    }

    public TemporalException()
    {
        super();
    }

    public TemporalException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public TemporalException( Throwable cause )
    {
        super( cause );
    }
}
