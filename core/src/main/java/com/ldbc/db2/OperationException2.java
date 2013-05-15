package com.ldbc.db2;

public class OperationException2 extends Exception
{
    private static final long serialVersionUID = 6646883591588721475L;

    public OperationException2( String message )
    {
        super( message );
    }

    public OperationException2()
    {
        super();
    }

    public OperationException2( String message, Throwable cause )
    {
        super( message, cause );
    }

    public OperationException2( Throwable cause )
    {
        super( cause );
    }
}
