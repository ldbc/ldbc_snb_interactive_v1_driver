package com.ldbc.driver;

public class DbException extends Exception
{
    private static final long serialVersionUID = 6646883591588721475L;

    public DbException( String message )
    {
        super( message );
    }

    public DbException()
    {
        super();
    }

    public DbException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public DbException( Throwable cause )
    {
        super( cause );
    }
}
