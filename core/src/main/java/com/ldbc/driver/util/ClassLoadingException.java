package com.ldbc.driver.util;

public class ClassLoadingException extends Exception
{
    private static final long serialVersionUID = 6646883591588721475L;

    public ClassLoadingException( String message )
    {
        super( message );
    }

    public ClassLoadingException()
    {
        super();
    }

    public ClassLoadingException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public ClassLoadingException( Throwable cause )
    {
        super( cause );
    }
}
