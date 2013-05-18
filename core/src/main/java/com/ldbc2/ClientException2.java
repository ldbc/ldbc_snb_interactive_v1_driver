package com.ldbc2;

public class ClientException2 extends Exception
{
    private static final long serialVersionUID = 7166804842129940500L;

    public ClientException2( String message )
    {
        super( message );
    }

    public ClientException2()
    {
        super();
    }

    public ClientException2( String message, Throwable cause )
    {
        super( message, cause );
    }

    public ClientException2( Throwable cause )
    {
        super( cause );
    }
}
