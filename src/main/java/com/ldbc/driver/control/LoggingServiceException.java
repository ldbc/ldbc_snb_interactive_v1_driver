package com.ldbc.driver.control;

public class LoggingServiceException extends Exception
{
    private static final long serialVersionUID = 6646883591588721475L;

    public LoggingServiceException( String message )
    {
        super( message );
    }

    public LoggingServiceException()
    {
        super();
    }

    public LoggingServiceException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public LoggingServiceException( Throwable cause )
    {
        super( cause );
    }
}
