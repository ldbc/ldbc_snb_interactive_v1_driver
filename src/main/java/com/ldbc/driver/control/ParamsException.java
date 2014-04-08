package com.ldbc.driver.control;

public class ParamsException extends Exception
{
    private static final long serialVersionUID = 8844396756042772132L;

    public ParamsException( String message )
    {
        super( message );
    }

    public ParamsException()
    {
        super();
    }

    public ParamsException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public ParamsException( Throwable cause )
    {
        super( cause );
    }
}
